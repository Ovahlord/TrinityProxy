using System.Buffers;
using System.IO.Pipelines;

namespace TrinityProxy.IO;

public class ProxyBridge
{
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly Stream _clientStream;
    private readonly Stream _serverStream;
    private int _closed;


    public ProxyBridge(Stream clientStream, Stream serverStream)
    {
        _cancellationTokenSource = new CancellationTokenSource();
        _clientStream = clientStream;
        _serverStream = serverStream;
        
        _ = RouteStreamDataAsync(clientStream, serverStream, _cancellationTokenSource.Token);
        _ = RouteStreamDataAsync(serverStream, clientStream, _cancellationTokenSource.Token);
    }

    private void Close()
    {
        // Both directions can fail at the same instant, and Cancel() after Dispose() throws,
        // so exactly one caller must be allowed through.
        if (Interlocked.Exchange(ref _closed, 1) == 1)
            return;

        _cancellationTokenSource.Cancel();
        _cancellationTokenSource.Dispose();
        _clientStream.Dispose();
        _serverStream.Dispose();
        Console.WriteLine("Bridge closed");
    }
    
    private async Task RouteStreamDataAsync(Stream fromStream, Stream toStream, CancellationToken cancellationToken = default)
    {
        PipeReader reader = PipeReader.Create(fromStream);
        PipeWriter writer = PipeWriter.Create(toStream);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    ReadResult result = await reader.ReadAsync(cancellationToken);
                    ReadOnlySequence<byte> buffer = result.Buffer;

                    foreach (ReadOnlyMemory<byte> segment in buffer)
                    {
                        writer.Write(segment.Span);
                    }

                    reader.AdvanceTo(buffer.End);

                    FlushResult flushResult = await writer.FlushAsync(cancellationToken);
                    if (flushResult.IsCompleted || result.IsCompleted)
                    {
                        Close();
                        return;
                    }
                }
                catch (Exception)
                {
                    // Cancellation included: either way this direction is finished, and
                    // continuing the loop would keep using streams Close() has disposed.
                    Close();
                    return;
                }
            }
        }
        finally
        {
            await reader.CompleteAsync();
            await writer.CompleteAsync();
        }
    }
    
}