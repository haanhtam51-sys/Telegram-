<?php

namespace danog\MadelineProto\Reactive;

use WeakMap;

/** @internal */
final class Emitter {

    /** @var WeakMap<Watcher, null> */
    private readonly WeakMap $map;

    public function __construct()
    {
        $this->map = new \WeakMap;
    }

    public function add(Watcher $w): void {
        $this->map[$w] = null;
    }

    public function update(): void {
        foreach ($this->map as $w => $_) {
            $w->updated();
        }
    }
}