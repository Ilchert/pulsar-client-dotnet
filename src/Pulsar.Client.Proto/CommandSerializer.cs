using ProtoBuf;
using pulsar.proto;
using Pulsar.Client.Common;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.IO.Pipelines;
using System.Threading.Tasks;

namespace Pulsar.Client.Proto
{
    internal static class CommandSerializer
    {
        private static readonly byte[] s_magicNumber = [14, 1];
        // Format
        // all numbers are big endian
        // |4 byte total length| 4 bytes command length|command|2 bytes magic number 0x0e01|4 bytes CRC32C|4 bytes metadata length|metadata|payload|

        public static ValueTask WritePayloadCommand(Stream stream, BaseCommand command, MessageMetadata metadata, MemoryStream payload)
        {
            // prepare crc data
            using var metadataMeasure = Serializer.Measure(metadata);

            var crc32Length = 4 + (int)metadataMeasure.Length + (int)payload.Length;
            using var crc32Buffer = MemoryPool<byte>.Shared.Rent(crc32Length);
            // write metadata length
            BinaryPrimitives.WriteInt32BigEndian(crc32Buffer.Memory.Span, (int)metadataMeasure.Length);

            var crc32BufferWriter = new MemoryBufferWriter(crc32Buffer.Memory[4..]);

            // write metadata
            metadataMeasure.Serialize(crc32BufferWriter);

            // write payload
            var payloadSpan = crc32BufferWriter.GetSpan();
            payload.Seek(0, SeekOrigin.Begin);
            payload.ReadExactly(payloadSpan[..(int)payload.Length]);
            crc32BufferWriter.Advance((int)payload.Length);

            var crc32c = CRC32C.Crc(crc32Buffer.Memory.Span[..crc32Length]);

            using var commandMeasure = Serializer.Measure(command);
            var totalLength = 4 + (int)commandMeasure.Length + 2 + 4 + crc32Length;

            var pipeWriter = PipeWriter.Create(stream);

            Span<byte> lengthBuffer = stackalloc byte[4];
            // write total length
            BinaryPrimitives.WriteInt32BigEndian(lengthBuffer, totalLength);
            pipeWriter.Write(lengthBuffer);

            // write command length
            BinaryPrimitives.WriteInt32BigEndian(lengthBuffer, (int)commandMeasure.Length);
            pipeWriter.Write(lengthBuffer);

            // write command
            commandMeasure.Serialize(pipeWriter);

            // write magic number
            pipeWriter.Write(s_magicNumber);

            // write crc32c value
            BinaryPrimitives.WriteUInt32BigEndian(lengthBuffer, crc32c);
            pipeWriter.Write(lengthBuffer);

            // write rest
            pipeWriter.Write(crc32Buffer.Memory.Span[..crc32Length]);
            return pipeWriter.CompleteAsync();
        }


        private class MemoryBufferWriter(Memory<byte> memory) : IBufferWriter<byte>
        {
            int _position = 0;

            public void Advance(int count) => _position += count;

            public Memory<byte> GetMemory(int sizeHint = 0) => memory[_position..];

            public Span<byte> GetSpan(int sizeHint = 0) => memory.Span[_position..];
        }
    }
}
