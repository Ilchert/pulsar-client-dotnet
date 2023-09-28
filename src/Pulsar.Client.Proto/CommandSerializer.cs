using ProtoBuf;
using pulsar.proto;
using Pulsar.Client.Common;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;

namespace Pulsar.Client.Proto
{
    internal static class CommandSerializer
    {
        private static readonly byte[] s_magicNumber = [14, 1];
        // Format
        // all numbers are big endian
        // |4 byte total length| 4 bytes command length|command|2 bytes magic number 0x0e01|4 bytes CRC32C|4 bytes metadata length|metadata|payload|

        public static void WritePayloadCommand(IBufferWriter<byte> bufferWriter, BaseCommand command, MessageMetadata metadata, MemoryStream payload)
        {
            // prepare crc data
            using var metadataMeasure = Serializer.Measure(metadata);
            using var commandMeasure = Serializer.Measure(command);

            var crc32Length = 4 + 4 + (int)metadataMeasure.Length + (int)payload.Length;
            var totalLength = 4 + (int)commandMeasure.Length + 2 + crc32Length;

            Span<byte> lengthBuffer = stackalloc byte[4];

            // write total length
            BinaryPrimitives.WriteInt32BigEndian(lengthBuffer, totalLength);
            bufferWriter.Write(lengthBuffer);

            // write command length
            BinaryPrimitives.WriteInt32BigEndian(lengthBuffer, (int)commandMeasure.Length);
            bufferWriter.Write(lengthBuffer);

            // write command
            commandMeasure.Serialize(bufferWriter);

            // write magic number
            bufferWriter.Write(s_magicNumber);

            var crc32Buffer = bufferWriter.GetMemory(crc32Length)[..crc32Length];
            var crc32Data = crc32Buffer[4..];

            // write metadata length
            BinaryPrimitives.WriteInt32BigEndian(crc32Data.Span, (int)metadataMeasure.Length);

            var crc32MetadataStart = crc32Data[4..];
            var crc32BufferWriter = new MemoryBufferWriter(crc32MetadataStart);

            // write metadata
            metadataMeasure.Serialize(crc32BufferWriter);

            // write payload
            payload.Seek(0, SeekOrigin.Begin);            
            payload.ReadExactly(crc32MetadataStart.Span[(int)metadataMeasure.Length..]);

            var crc32c = CRC32C.Crc(crc32Data.Span);

            // write crc32c value to start buffer
            BinaryPrimitives.WriteUInt32BigEndian(crc32Buffer.Span, crc32c);

            bufferWriter.Advance(crc32Length);
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
