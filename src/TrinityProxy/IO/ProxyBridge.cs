using System.Buffers;
using System.IO.Pipelines;

namespace TrinityProxy.IO;

public class ProxyBridge
{
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly Stream _clientStream;
    private readonly Stream _serverStream;
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _closed;

    /// <summary>Completes once the bridge has been closed from either side.</summary>
    public Task Completion => _completion.Task;


    public ProxyBridge(Stream clientStream, Stream serverStream)
    {
        _cancellationTokenSource = new CancellationTokenSource();
        _clientStream = clientStream;
        _serverStream = serverStream;

        // Read the token once up front: the first direction can run to completion synchronously
        // (an already closed peer yields EOF without ever suspending) and close the bridge
        // before the second direction is even started.
        CancellationToken cancellationToken = _cancellationTokenSource.Token;

        _ = RouteStreamDataAsync(clientStream, serverStream, cancellationToken);
        _ = RouteStreamDataAsync(serverStream, clientStream, cancellationToken);
    }

    public void Close()
    {
        // Both directions can fail at the same instant, and Cancel() after Dispose() throws,
        // so exactly one caller must be allowed through.
        if (Interlocked.Exchange(ref _closed, 1) == 1)
            return;

        // Not disposed here: the other direction may still be awaiting on this token.
        // Nothing schedules a timer on it, so letting it be collected costs nothing.
        _cancellationTokenSource.Cancel();
        _clientStream.Dispose();
        _serverStream.Dispose();
        _completion.TrySetResult();
        Console.WriteLine("Bridge closed");
    }
    
    private async Task RouteStreamDataAsync(Stream fromStream, Stream toStream, CancellationToken cancellationToken = default)
    {
        // leaveOpen: the bridge owns both streams and disposes them in Close(). Without this the
        // pipes dispose them on completion, tearing them out from under the other direction.
        PipeReader reader = PipeReader.Create(fromStream, new StreamPipeReaderOptions(leaveOpen: true));
        PipeWriter writer = PipeWriter.Create(toStream, new StreamPipeWriterOptions(leaveOpen: true));

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