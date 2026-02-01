<?php declare(strict_types=1);

use danog\MadelineProto\Logger;
use danog\MadelineProto\RPCErrorException;
use danog\MadelineProto\Settings\Logger as SettingsLogger;
use danog\MadelineProto\Settings\TLSchema;
use danog\MadelineProto\TL\TL;
use Revolt\EventLoop;
use Webmozart\Assert\Assert;

use function Amp\async;

/*
Copyright 2016-2020 Daniil Gentili
(https://daniil.it)
This file is part of MadelineProto.
MadelineProto is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
MadelineProto is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU Affero General Public License for more details.
You should have received a copy of the GNU General Public License along with MadelineProto.
If not, see <http://www.gnu.org/licenses/>.
 */

require 'vendor/autoload.php';
$logger = new Logger(new SettingsLogger);

set_error_handler(['\danog\MadelineProto\Exception', 'ExceptionErrorHandler']);

/**
 * @internal
 */
function getTLSchema(): TLSchema
{
    $layerFile = glob(__DIR__."/../src/TL_telegram_v*.tl")[0];
    return (new TLSchema)->setAPISchema($layerFile)->setSecretSchema('')->setFuzzMode(true);
}

/**
 * Get TL info of layer.
 *
 * @internal
 *
 * @return void
 */
function getTL(TLSchema $schema)
{
    $layer = new TL();
    $layer->init($schema);

    return ['methods' => $layer->getMethods(), 'constructors' => $layer->getConstructors()];
}
$schema = getTLSchema();
$layer = getTL($schema);
$res = '';

echo "Bot login:".PHP_EOL;
$bot = new \danog\MadelineProto\API('fuzz_bot.madeline');
$bot->start();
$bot->updateSettings($schema);
Assert::true($bot->isSelfBot(), "bot.madeline is not a bot!");
$bot->restart();

echo "User login:".PHP_EOL;
$user = new \danog\MadelineProto\API('fuzz_user.madeline');
$user->start();
$user->updateSettings($schema);
Assert::true($user->isSelfUser(), "user.madeline is not a user!");
$user->restart();

$methods = [];
foreach ($layer['methods']->by_id as $constructor) {
    $name = $constructor['method'];
    if (strtolower($name) === 'account.deleteaccount'
        || strtolower($name) === 'auth.logout'
        || $name === 'auth.resetAuthorizations'
        || $name === 'auth.dropTempAuthKeys'
        || $name === 'account.resetAuthorization'
        || $name === 'account.resetPassword'
        || $name === 'account.updateUsername'
        || $name === 'photos.updateProfilePhoto'
        || $name === 'photos.uploadProfilePhoto'
        || !str_contains($name, '.')) {
        continue;
    }
    [$namespace, $method] = explode('.', $name);

    $methods["bot $name"]= async(static function () use ($namespace, $method, $bot, $name, &$methods): void {
        try {
            $bot->{$namespace}->{$method}();
        } catch (RPCErrorException) {
        }
        unset($methods["bot $name"]);
    });
    $methods["user $name"] = async(static function () use ($namespace, $method, $user, $name, &$methods): void {
        try {
            $user->{$namespace}->{$method}();
        } catch (RPCErrorException) {
        }
        unset($methods["user $name"]);
    });
}

EventLoop::repeat(1.0, static function (string $id) use (&$methods): void {
    if (!$methods) {
        echo "Done processing!".PHP_EOL;
        EventLoop::cancel($id);
        return;
    }
    echo "Processing ".implode(", ", array_keys($methods)).PHP_EOL;
});

\Amp\Future\awaitAll($methods);
