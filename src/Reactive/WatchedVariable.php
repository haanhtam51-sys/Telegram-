<?php

namespace danog\MadelineProto\Reactive;

/** 
 * @template T
 * @internal
 */
final class WatchedVariable {

    private Emitter $emitter;

    public function __construct(
        /** @var T */
        private mixed $value
    )
    {
        $this->emitter = new Emitter;
    }

    public function __sleep()
    {
        return ['value'];
    }

    public function __wakeup()
    {
        $this->emitter = new Emitter;
    }

    public function watch(Watcher $w): void {
        $this->emitter->add($w);
    }

    /**
     * @return T
     */
    public function get(): mixed {
        return $this->value;
    }

    /**
     * @param T $value
     */
    public function set(mixed $value): void {
        $this->value = $value;
        $this->emitter->update();
    }
}