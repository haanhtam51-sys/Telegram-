<?php

declare(strict_types=1);

/**
 * Socket write loop.
 *
 * This file is part of MadelineProto.
 * MadelineProto is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 * MadelineProto is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU General Public License along with MadelineProto.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * @author    Daniil Gentili <daniil@daniil.it>
 * @copyright 2016-2023 Daniil Gentili <daniil@daniil.it>
 * @license   https://opensource.org/licenses/AGPL-3.0 AGPLv3
 * @link https://docs.madelineproto.xyz MadelineProto documentation
 */

namespace danog\MadelineProto\Loop\Connection;

use Amp\ByteStream\StreamException;
use Amp\DeferredFuture;
use danog\Loop\Loop;
use danog\MadelineProto\Connection;
use danog\MadelineProto\Logger;
use danog\MadelineProto\MTProto\Container;
use danog\MadelineProto\MTProto\MTProtoOutgoingMessage;
use danog\MadelineProto\MTProtoTools\Crypt;
use danog\MadelineProto\Reactive\Watcher;
use danog\MadelineProto\Tools;
use Revolt\EventLoop;

use function strlen;

/**
 * Socket write loop.
 *
 * @internal
 *
 * @author Daniil Gentili <daniil@daniil.it>
 */
final class WriteLoop extends Loop
{
    private const MAX_COUNT = 1020;
    private const MAX_SIZE = 1 << 15;
    private const LONG_POLL_TIMEOUT = 30.0;
    private const LONG_POLL_TIMEOUT_MS = 30_000;
    public const MAX_IDS = 8192;

    use Common {
        __construct as init2;
    }

    private readonly bool $isHttp;

    /**
     * Constructor function.
     */
    public function __construct(Connection $connection)
    {
        $this->init2($connection);
    }
    /**
     * Main loop.
     */
    public function loop(): ?float
    {
        $please_wait = false;
        $first = true;
        while (true) {
            if ($this->connection->shouldReconnect()) {
                $this->API->logger("Exiting $this because connection is old");
                return self::STOP;
            }
            if (!$this->connection->pendingOutgoing && !$first) {
                $this->API->logger("No messages, pausing in $this...", Logger::ULTRA_VERBOSE);
                return self::LONG_POLL_TIMEOUT;
            }
            if ($please_wait) {
                $this->API->logger("Have to wait for handshake, pausing in $this...", Logger::ULTRA_VERBOSE);
                return 1.0;
            }
            $this->connection->writing(true);
            try {
                $please_wait = $this->shared->hasTempAuthKey()
                    ? $this->encryptedWriteLoop($first)
                    : $this->unencryptedWriteLoop();
            } catch (StreamException $e) {
                if ($this->connection->shouldReconnect()) {
                    $this->API->logger("Stopping $this because we have to reconnect");
                    return self::STOP;
                }
                EventLoop::queue(function () use ($e): void {
                    $this->API->logger($e);
                    $this->API->logger("Got nothing in the socket in DC {$this->datacenter}, reconnecting...", Logger::ERROR);
                    $this->connection->reconnect();
                });
                $this->API->logger("Stopping $this");
                return self::STOP;
            } catch (\Throwable $e) {
                $this->API->logger("Exiting $this due to $e", Logger::FATAL_ERROR);
                return self::STOP;
            } finally {
                $this->connection->writing(false);
            }
            $first = false;
        }
    }
    public function unencryptedWriteLoop(): bool
    {
        if ($queue = $this->connection->unencrypted_check_queue) {
            $this->connection->unencrypted_check_queue = [];
            foreach ($queue as $msg) {
                $this->connection->methodRecall($msg);
            }
        }
        while ($this->connection->pendingOutgoing) {
            $skipped_all = true;
            foreach ($this->connection->pendingOutgoing as $k => $message) {
                if ($this->shared->hasTempAuthKey()) {
                    return false;
                }
                if (!$message->unencrypted) {
                    continue;
                }
                if ($message->getState() & MTProtoOutgoingMessage::STATE_REPLIED) {
                    unset($this->connection->pendingOutgoing[$k]);
                    $this->connection->pendingOutgoingGauge?->set(\count($this->connection->pendingOutgoing));
                    continue;
                }
                $skipped_all = false;
                $this->API->logger("Sending $message as unencrypted message to DC $this->datacenter", Logger::ULTRA_VERBOSE);
                $message_id = $message->getMsgId() ?? $this->connection->msgIdHandler->generateMessageId();
                $length = \strlen($message->getSerializedBody());
                $pad_length = -$length & 15;
                $pad_length += 16 * Tools::randomInt(modulus: 16);
                $pad = Tools::random($pad_length);
                $buffer = $this->connection->stream->getWriteBuffer($total_len = 8 + 8 + 4 + $pad_length + $length);
                $buffer->bufferWrite("\0\0\0\0\0\0\0\0".Tools::packSignedLong($message_id).Tools::packUnsignedInt($length).$message->getSerializedBody().$pad);
                $this->connection->httpSent();
                $this->connection->outgoingBytesCtr?->incBy($total_len);

                $this->API->logger("Sent $message as unencrypted message to DC $this->datacenter!", Logger::ULTRA_VERBOSE);

                unset($this->connection->pendingOutgoing[$k]);
                $this->connection->pendingOutgoingGauge?->set(\count($this->connection->pendingOutgoing));
                $message->setMsgId($message_id);
                $message->sent();
            }
            if ($skipped_all) {
                return true;
            }
        }
        return false;
    }
    public function encryptedWriteLoop(bool $first): bool
    {
        do {
            if (!$this->shared->hasTempAuthKey()) {
                return false;
            }
            if (!$first && !$this->connection->pendingOutgoing) {
                return false;
            }

            if ($check = $this->connection->check_queue) {
                $this->connection->check_queue = [];
                $deferred = new DeferredFuture();
                $list = '';
                $msgIds = [];
                foreach ($check as $msg) {
                    if ($msg->hasReply()) {
                        continue;
                    }
                    $id = $msg->getMsgId();
                    if ($id === null) {
                        $this->API->logger("$msg has no ID, cannot request status!", Logger::ERROR);
                        continue;
                    }
                    $msgIds[] = $id;
                    $list .= $msg->constructor.', ';
                }
                $this->API->logger("Still missing {$list} on DC {$this->datacenter}, sending state request", Logger::ERROR);
                $this->connection->objectCall('msgs_state_req', ['msg_ids' => $msgIds], $deferred);
                EventLoop::queue(function () use ($deferred, $check): void {
                    try {
                        $result = $deferred->getFuture()->await(new TimeoutCancellation($this->pollTimeout));
                        if (\is_callable($result)) {
                            throw $result();
                        }
                        $reply = [];
                        foreach (str_split($result['info']) as $key => $chr) {
                            $message = $check[$key];
                            if ($message->hasReply()) {
                                $this->API->logger("Already got response for and forgot about message ID $message_id");
                                continue;
                            }
                            $chr = \ord($chr);
                            switch ($chr & 7) {
                                case 0:
                                    $this->API->logger("Wrong message status 0 for $message", Logger::FATAL_ERROR);
                                    break;
                                case 1:
                                case 2:
                                case 3:
                                    if ($message->constructor === 'msgs_state_req') {
                                        $message->reply(null);
                                        break;
                                    }
                                    $this->API->logger("Message $message not received by server, resending...", Logger::ERROR);
                                    $this->connection->methodRecall($message);
                                    break;
                                case 4:
                                    if ($chr & 128) {
                                        $this->API->logger("Message $message received by server and was already sent.", Logger::ERROR);
                                    } elseif ($chr & 64) {
                                        $this->API->logger("Message $message received by server and was already processed.", Logger::ERROR);
                                    } elseif ($chr & 32) {
                                        if ($message->getSent() + $this->resendTimeout < hrtime(true)) {
                                            if (!$message->cancellation?->isRequested()) {
                                                $this->API->logger("Message $message received by server and is being processed for way too long, resending request...", Logger::ERROR);
                                                $this->connection->methodRecall($message);
                                            }
                                        } else {
                                            $this->API->logger("Message $message received by server and is being processed, waiting...", Logger::ERROR);
                                        }
                                    } else {
                                        $this->API->logger("Message $message received by server, waiting...", Logger::ERROR);
                                    }
                                    break;
                            }
                        }
                        //} catch (CancelledException) {
                        //$this->API->logger("We did not receive a response for {$this->pollTimeout} seconds: reconnecting and exiting check loop on DC {$this->datacenter}");
                        //EventLoop::queue($this->connection->reconnect(...));
                    } catch (\Throwable $e) {
                        $this->API->logger("Got exception in check loop for DC {$this->datacenter}");
                        $this->API->logger((string) $e);
                    }
                });
            }

            ksort($this->connection->pendingOutgoing);

            $messages = [];
            $keys = [];

            $total_length = 0;
            $count = 0;
            $skipped = false;

            $has_seq = false;

            $has_state = false;
            $has_resend = false;
            foreach ($this->connection->pendingOutgoing as $k => $message) {
                if ($message->unencrypted) {
                    continue;
                }
                if ($message->getState() & MTProtoOutgoingMessage::STATE_REPLIED) {
                    unset($this->connection->pendingOutgoing[$k]);
                    $this->connection->pendingOutgoingGauge?->set(\count($this->connection->pendingOutgoing));
                    $this->API->logger("Skipping resending of $message, we already got a reply in DC $this->datacenter");
                    continue;
                }
                if ($message instanceof Container) {
                    unset($this->connection->pendingOutgoing[$k]);
                    $this->connection->pendingOutgoingGauge?->set(\count($this->connection->pendingOutgoing));
                    continue;
                }
                $constructor = $message->constructor;
                if ($this->shared->getGenericSettings()->getAuth()->getPfs() && !$this->shared->isBound() && !$this->connection->isCDN() && $message->isMethod && $constructor !== 'auth.bindTempAuthKey') {
                    $this->API->logger("Skipping $message due to unbound keys in DC $this->datacenter");
                    $skipped = true;
                    continue;
                }
                if ($constructor === 'msgs_state_req') {
                    if ($has_state) {
                        $this->API->logger("Already have a state request queued for the current container in DC {$this->datacenter}");
                        continue;
                    }
                    $has_state = true;
                }
                if ($constructor === 'msg_resend_req') {
                    if ($has_resend) {
                        continue;
                    }
                    $has_resend = true;
                }

                $body_length = \strlen($message->getSerializedBody());
                $actual_length = $body_length + 32;
                if ($total_length && $total_length + $actual_length > 32760 || $count >= self::MAX_COUNT) {
                    $this->API->logger('Length overflow, postponing part of payload', Logger::ULTRA_VERBOSE);
                    break;
                }
                if ($message->hasSeqNo()) {
                    $has_seq = true;
                }

                $message_id = $message->getMsgId() ?? $this->connection->msgIdHandler->generateMessageId();
                $this->API->logger("Sending $message as encrypted message to DC $this->datacenter", Logger::ULTRA_VERBOSE);
                $MTmessage = [
                    '_' => 'MTmessage',
                    'msg_id' => $message_id,
                    'body' => $message->getSerializedBody(),
                    'seqno' => $message->getSeqNo() ?? $this->connection->generateOutSeqNo($message->contentRelated),
                ];
                if ($message->isMethod && $constructor !== 'auth.bindTempAuthKey') {
                    $isInited = Watcher::from(function (Watcher $w): \Closure {
                        $this->shared->getTempAuthKey()->watchInited($w);
                        return fn (): bool => $this->shared->getTempAuthKey()->isInited();
                    });
                    if (!$isInited) {
                        if ($constructor === 'help.getConfig' || $constructor === 'upload.getCdnFile') {
                            $this->API->logger(sprintf('Writing client info (also executing %s)...', $constructor), Logger::NOTICE);
                            $MTmessage['body'] = ($this->API->getTL()->serializeMethod('invokeWithLayer', [
                                'layer' => $this->API->settings->getSchema()->getLayer(),
                                'query' => $this->API->getTL()->serializeMethod(
                                    'initConnection',
                                    [
                                        'api_id' => $this->API->settings->getAppInfo()->getApiId(),
                                        'api_hash' => $this->API->settings->getAppInfo()->getApiHash(),
                                        'device_model' => !$this->connection->isCDN() ? $this->API->settings->getAppInfo()->getDeviceModel() : 'n/a',
                                        'system_version' => !$this->connection->isCDN() ? $this->API->settings->getAppInfo()->getSystemVersion() : 'n/a',
                                        'app_version' => $this->API->settings->getAppInfo()->getAppVersion(),
                                        'system_lang_code' => $this->API->settings->getAppInfo()->getSystemLangCode(),
                                        'lang_code' => $this->API->settings->getAppInfo()->getLangCode(),
                                        'lang_pack' => $this->API->settings->getAppInfo()->getLangPack(),
                                        'proxy' => $this->connection->getInputClientProxy(),
                                        'query' => $MTmessage['body'],
                                    ]
                                ),
                            ]));
                        } else {
                            $this->API->logger("Skipping $message due to uninited connection in DC $this->datacenter");
                            $skipped = true;
                            continue;
                        }
                    } elseif ($this->API->authorized === \danog\MadelineProto\API::LOGGED_IN
                        && !$this->shared->isAuthorized()
                        && $constructor !== 'auth.importAuthorization'
                        && !$this->connection->isCDN()
                    ) {
                        $this->API->logger("Skipping $message due to unimported auth in connection in DC $this->datacenter");
                        $skipped = true;
                        continue;
                    } elseif (($prev = $message->previousQueuedMessage)
                        && !$prev->hasReply()
                    ) {
                        $prevId = $prev->getMsgId();
                        if (!$prevId) {
                            $prev->getResultPromise()->finally(fn () => $this->resume(true));
                            $this->API->logger("Skipping $message due pending local queue in DC $this->datacenter");
                            $skipped = true;
                            continue;
                        }
                        $MTmessage['body'] = $this->API->getTL()->serializeMethod(
                            'invokeAfterMsg',
                            [
                                'msg_id' => $prevId,
                                'query' => $MTmessage['body'],
                            ]
                        );
                    }
                    // TODO
                    /*
                    if ($this->API->settings['requests']['gzip_encode_if_gt'] !== -1 && ($l = strlen($MTmessage['body'])) > $this->API->settings['requests']['gzip_encode_if_gt']) {
                        if (($g = strlen($gzipped = gzencode($MTmessage['body']))) < $l) {
                            $MTmessage['body'] = $this->API->getTL()->serializeObject(['type' => ''], ['_' => 'gzip_packed', 'packed_data' => $gzipped], 'gzipped data');
                            $this->API->logger('Using GZIP compression for ' . $constructor . ', saved ' . ($l - $g) . ' bytes of data, reduced call size by ' . $g * 100 / $l . '%', \danog\MadelineProto\Logger::ULTRA_VERBOSE);
                        }
                        unset($gzipped);
                    }*/
                }
                $body_length = \strlen($MTmessage['body']);
                $actual_length = $body_length + 32;
                if ($total_length && $total_length + $actual_length > 32760) {
                    $this->API->logger('Length overflow, postponing part of payload', Logger::ULTRA_VERBOSE);
                    break;
                }
                $count++;
                $total_length += $actual_length;
                $MTmessage['bytes'] = $body_length;
                $messages[] = $MTmessage;
                $keys[$k] = $message;

                $message->setSeqNo($MTmessage['seqno'])
                        ->setMsgId($MTmessage['msg_id']);
            }
            $MTmessage = null;

            $acks = \array_slice($this->connection->ack_queue, 0, self::MAX_COUNT);
            if ($ackCount = \count($acks)) {
                $this->API->logger('Adding msgs_ack', Logger::ULTRA_VERBOSE);

                $body = $this->API->getTL()->serializeObject(['type' => ''], ['_' => 'msgs_ack', 'msg_ids' => $acks], 'msgs_ack');
                $messages []= [
                    '_' => 'MTmessage',
                    'msg_id' => $this->connection->msgIdHandler->generateMessageId(),
                    'body' => $body,
                    'seqno' => $this->connection->generateOutSeqNo(false),
                    'bytes' => \strlen($body),
                ];
                $count++;
                unset($acks, $body);
            }
            if ($this->connection->isHttp()) {
                $this->API->logger('Adding http_wait', Logger::ULTRA_VERBOSE);
                $body = $this->API->getTL()->serializeObject(['type' => ''], ['_' => 'http_wait', 'max_wait' => 30000, 'wait_after' => 0, 'max_delay' => 0], 'http_wait');
                $messages []= [
                    '_' => 'MTmessage',
                    'msg_id' => $this->connection->msgIdHandler->generateMessageId(),
                    'body' => $body,
                    'seqno' => $this->connection->generateOutSeqNo(true),
                    'bytes' => \strlen($body),
                ];
                $count++;
                unset($body);
            }

            if ($count > 1 || $has_seq) {
                $this->API->logger("Wrapping in msg_container ({$count} messages of total size {$total_length}) as encrypted message for DC {$this->datacenter}", Logger::ULTRA_VERBOSE);
                $message_id = $this->connection->msgIdHandler->generateMessageId();
                $this->connection->pendingOutgoing[$this->connection->pendingOutgoingKey] = $ct = new Container($this->connection, $keys);
                $this->connection->outgoingCtr?->inc();
                $this->connection->pendingOutgoingGauge?->set(\count($this->connection->pendingOutgoing));
                $keys[$this->connection->pendingOutgoingKey++] = $ct;
                $message_data = $this->API->getTL()->serializeObject(['type' => ''], ['_' => 'msg_container', 'messages' => $messages], 'container');
                $message_data_length = \strlen($message_data);
                $seq_no = $this->connection->generateOutSeqNo(false);
            } elseif ($count) {
                $message = $messages[0];
                $message_data = $message['body'];
                $message_data_length = $message['bytes'];
                $message_id = $message['msg_id'];
                $seq_no = $message['seqno'];
            } else {
                $this->API->logger("NO MESSAGE SENT in $this, pending ".implode(', ', array_map('strval', $this->connection->pendingOutgoing)), Logger::WARNING);
                return true;
            }
            unset($messages);
            $plaintext = $this->shared->getTempAuthKey()->getServerSalt().$this->connection->session_id.Tools::packSignedLong($message_id).pack('VV', $seq_no, $message_data_length).$message_data;
            $padding = Tools::posmod(-\strlen($plaintext), 16);
            if ($padding < 12) {
                $padding += 16;
            }
            $padding = Tools::random($padding);
            $message_key_large = hash('sha256', substr($this->shared->getTempAuthKey()->getAuthKey(), 88, 32).$plaintext.$padding, true);
            $message_key = substr($message_key_large, 8, 16);
            //$ack = unpack('V', substr($message_key_large, 0, 4))[1] | (1 << 31);
            [$aes_key, $aes_iv] = Crypt::kdf($message_key, $this->shared->getTempAuthKey()->getAuthKey());
            $message = $this->shared->getTempAuthKey()->getID().$message_key.Crypt::igeEncrypt($plaintext.$padding, $aes_key, $aes_iv);
            $buffer = $this->connection->stream->getWriteBuffer($total_len = \strlen($message));
            $buffer->bufferWrite($message);
            $this->connection->httpSent();
            $this->connection->outgoingBytesCtr?->incBy($total_len);
            $this->API->logger("Sent encrypted payload to DC {$this->datacenter}", Logger::ULTRA_VERBOSE);

            if ($ackCount) {
                $this->connection->ack_queue = \array_slice($this->connection->ack_queue, $ackCount);
            }

            foreach ($keys as $key => $message) {
                unset($this->connection->pendingOutgoing[$key]);
                $message->sent();
            }
            $this->connection->pendingOutgoingGauge?->set(\count($this->connection->pendingOutgoing));
        } while ($this->connection->pendingOutgoing && !$skipped);
        if (empty($this->connection->pendingOutgoing)) {
            $this->connection->pendingOutgoing = [];
            $this->connection->pendingOutgoingKey = 0;
        }
        return $skipped;
    }
    /**
     * Get loop name.
     */
    public function __toString(): string
    {
        return "write loop in DC {$this->datacenter}";
    }
}
