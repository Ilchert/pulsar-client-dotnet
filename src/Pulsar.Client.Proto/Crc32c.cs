using System;
using System.IO;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.IO;

namespace Pulsar.Client.Common
{
    internal static class CRC32C
    {
        public static uint Crc(ReadOnlySpan<byte> source)
        {
            var crc = ~0U; //0xFFFFFFFF
            if (source.Length >= sizeof(ulong))
            {
                ref byte ptr = ref MemoryMarshal.GetReference(source);
                int longLength = source.Length & ~0x7; // Exclude trailing bytes not a multiple of 8

                for (int i = 0; i < longLength; i += sizeof(ulong))
                {
                    crc = BitOperations.Crc32C(crc,
                        Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref ptr, i)));
                }

                source = source[longLength..];
            }

            // Compute remaining bytes
            for (int i = 0; i < source.Length; i++)
            {
                crc = BitOperations.Crc32C(crc, source[i]);
            }
            return crc ^ ~0U; //0xFFFFFFFF
        }

        internal static uint GetForRMS(RecyclableMemoryStream stream, int size)
        {
            var crc = ~0U; //0xFFFFFFFF
            var memorySequence = stream.GetReadOnlySequence().Slice(stream.Position);
            foreach (var memory in memorySequence)
            {
                var span = memory.Span;
                crc = CrcAlgorithm(size, span, crc);
            }
            return crc ^ ~0U; //0xFFFFFFFF
        }

        private static uint CrcAlgorithm(int size, ReadOnlySpan<byte> span, uint crc)
        {
            var currentBlockLength = span.Length;
            var i = 0;
            var bigStepsCount = currentBlockLength / 8;
            while (i < bigStepsCount)
            {
                var start = i * 8;
                var batch = BitConverter.ToUInt64(span.Slice(start, 8));
                crc = BitOperations.Crc32C(crc, batch);
                i++;
            }

            i = bigStepsCount * 8;
            while (size > 0 && i < currentBlockLength)
            {
                crc = BitOperations.Crc32C(crc, span[i]);
                size--;
                i++;
            }
            return crc;
        }

        internal static uint GetForMS(MemoryStream stream, int size)
        {
            var crc = ~0U; //0xFFFFFFFF
            var buf = stream.GetBuffer();
            var offset = (int)stream.Position;
            var span = buf.AsSpan(offset);
            crc = CrcAlgorithm(size, span, crc);
            return crc ^ ~0U; //0xFFFFFFFF
        }
    }
}

