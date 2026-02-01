<?php

namespace danog\MadelineProto\Reactive;

use Revolt\EventLoop;
use SplObjectStorage;

/** 
 * @internal
 * 
 * @template T
 */
final class Watcher {
    /**
     * @var SplObjectStorage<\Closure, self>
     */
    private static SplObjectStorage $db;

    /** @var T */
    private mixed $value;
    /**
     * @var \Closure(): T
     */
    private readonly \Closure $getter;
    /**
     * @param \Closure(self): (\Closure(): T) $register
     */
    private function __construct(\Closure $register)
    {
        $this->getter = $register($this);
        $this->value = ($this->getter)();
    }

    private ?string $id = null;

    /** @internal */
    public function updated(): void {
        if ($this->id !== null) {
            $this->id = EventLoop::defer(function () {
                $this->id = null;
                $this->value = ($this->getter)();
            });
        }
    }

    /**
     * @template TT
     * @param \Closure(self): (\Closure(): TT) $cb
     * @return TT
     */
    public static function from(\Closure $cb): mixed
    {
        if (isset(self::$db[$cb])) {
            return self::$db[$cb]->value;
        }
        self::$db[$cb] = new self($cb);
        return self::$db[$cb]->value;
    }
}