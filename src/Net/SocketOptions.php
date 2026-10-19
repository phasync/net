<?php

namespace phasync\Net;

/**
 * @internal Shared option handling for listen(), serve(), listenPacket() and dial(): one flat
 * option shape, PHP's own stream-context names, the same for all four. Unknown option names
 * fail loudly instead of being silently ignored, and a 'context' key escapes to raw stream
 * context options (such as 'ssl') for anything phasync/net does not name itself.
 */
final class SocketOptions
{
    private static ?int $backlog = null;

    /**
     * The default backlog: net.core.somaxconn on Linux, capped at 65535 (older kernels store
     * the backlog in 16 bits, so a larger value wraps), or 511 where that file cannot be read
     * (macOS, BSD, Windows). Read once per process.
     */
    public static function defaultBacklog(): int
    {
        return self::$backlog ??= self::readBacklog();
    }

    private static function readBacklog(): int
    {
        $value = @\file_get_contents('/proc/sys/net/core/somaxconn');

        return false !== $value && '' !== ($value = \trim($value)) ? \min((int) $value, 65535) : 511;
    }

    /**
     * Split $options into phasync/net's own flat options (merged with $defaults for the ones
     * missing) and the raw stream context under 'context'. Throws loudly on any option name
     * other than those in $allowed or 'context'.
     *
     * @param list<string> $allowed
     *
     * @return array{0: array, 1: array} [flat options with defaults applied, raw context]
     */
    public static function normalize(array $options, array $allowed, array $defaults): array
    {
        $permitted = [...$allowed, 'context'];
        $unknown   = \array_diff(\array_keys($options), $permitted);
        if ($unknown) {
            throw new \InvalidArgumentException(
                'Unknown option' . (\count($unknown) > 1 ? 's' : '') . ' ' . \implode(', ', $unknown)
                . '; expected ' . \implode(', ', $permitted)
            );
        }
        $context = $options['context'] ?? [];
        unset($options['context']);

        return [$options + $defaults, $context];
    }
}
