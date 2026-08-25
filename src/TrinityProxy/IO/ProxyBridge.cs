using System.Buffers;
using System.IO.Pipelines;

namespace TrinityProxy.IO;

public class ProxyBridge
{
    private readonly CancellationTokenSource _cancellationTokenSource;
    
    public ProxyBridge(Stream clientStream, Stream serverStream)
    {
        _cancellationTokenSource = new CancellationTokenSource();
        _ = RouteStreamDataAsync(clientStream, serverStream, _cancellationTokenSource.Token);
        _ = RouteStreamDataAsync(serverStream, clientStream, _cancellationTokenSource.Token);
    }

    private void Close()
    {
        if (_cancellationTokenSource.IsCancellationRequested)
            return;
        
        _cancellationTokenSource.Cancel();
        _cancellationTokenSource.Dispose();
        Console.WriteLine("Bridge closed");
    }
    
    private async Task RouteStreamDataAsync(Stream fromStream, Stream toStream, CancellationToken cancellationToken = default)
    {
        await using (fromStream)
        {
            await using (toStream)
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
                                Console.WriteLine("Bridge closed");
                            }
                            
                        }
                        catch (OperationCanceledException)
                        {
                        }
                        catch (Exception)
                        {
                            Close();
                            Console.WriteLine("Bridge closed");
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
    }
    
}