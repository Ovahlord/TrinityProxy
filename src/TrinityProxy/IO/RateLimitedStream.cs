using System.Threading.RateLimiting;

namespace TrinityProxy.IO;

public class RateLimitedStream : Stream
{
    private readonly Stream _innerStream;
    private readonly SlidingWindowRateLimiter _rateLimiter;
    private readonly int _maxBytesPerRead;
    private volatile bool _disposed;
    
    public RateLimitedStream(Stream innerStream, SlidingWindowRateLimiterOptions rateLimiterOptions, int maxBytesPerRead)
    {
        _innerStream = innerStream;
        _rateLimiter = new SlidingWindowRateLimiter(rateLimiterOptions);
        _maxBytesPerRead = maxBytesPerRead;
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        int bytesPerRead = Math.Min(_maxBytesPerRead, buffer.Length);
        
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                 using RateLimitLease lease = await _rateLimiter.AcquireAsync(bytesPerRead, cancellationToken);
                 if (lease.IsAcquired)
                     return await _innerStream.ReadAsync(buffer.Slice(0, bytesPerRead), cancellationToken);

                 // If the rate limiter tokens are exhausted, try again in 100ms 
                 await Task.Delay(100, cancellationToken);
            }
            catch (OperationCanceledException)
            {
            }
        }
        
        return 0;
    }

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        await ReadAsync(buffer.AsMemory(offset, count), cancellationToken);

    public override void Flush() => _innerStream.Flush();
    public override int Read(byte[] buffer, int offset, int count) => _innerStream.Read(buffer, offset, count);
    public override long Seek(long offset, SeekOrigin origin) => _innerStream.Seek(offset, origin);
    public override void SetLength(long value) => _innerStream.SetLength(value);
    public override void Write(byte[] buffer, int offset, int count) => _innerStream.Write(buffer, offset, count);

    public override bool CanRead =>  _innerStream.CanRead;
    public override bool CanSeek => _innerStream.CanSeek;
    public override bool CanWrite => _innerStream.CanWrite;
    public override long Length => _innerStream.Length;
    public override long Position
    {
        get => _innerStream.Position;
        set => _innerStream.Position = value;
    }

    protected override void Dispose(bool disposing)
    {
        if (Interlocked.Exchange(ref _disposed, true))
            return;

        if (disposing)
        {
            _innerStream.Dispose();
            _rateLimiter.Dispose();
        }
        
        base.Dispose(disposing);
    }
}