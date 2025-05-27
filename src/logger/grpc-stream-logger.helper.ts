import { Observable, Subscriber } from 'rxjs';
import { Metadata } from '@grpc/grpc-js';

export class GrpcStreamLoggerHelper {
  static getMetadataValues(metadata: Metadata) {
    return {
      userAgent: (metadata.get('user-agent')?.[0] as string) || 'Unknown',
      ipAddress: (metadata.get('ip-address')?.[0] as string) || 'Unknown',
      dns: (metadata.get('dns')?.[0] as string) || 'Unknown',
      userId: metadata.get('user-id')?.[0] as string || 'Unknown',
    };
  }

  static createStreamHandler<T>(
    streamName: string,
    metadata: Metadata,
    logger: any,
    stream$: Observable<T>,
    getLogContext: (data: T) => Record<string, any> = () => ({}),
  ): Observable<T> {
    const { userAgent, ipAddress, dns, userId } =
      this.getMetadataValues(metadata);

    const requestId = logger.initializeRequest(
      streamName,
      userAgent,
      ipAddress,
      dns,
      userId,
    );

    return new Observable((subscriber: Subscriber<T>) => {
      stream$.subscribe({
        next: (data: T) => {
          logger.logStreamEvent(requestId, 'STREAM_DATA', getLogContext(data));
          subscriber.next(data);
        },
        error: (err) => {
          logger.logError(requestId, err);
          subscriber.error(err);
        },
        complete: () => {
          logger.finalizeRequest(requestId);
          subscriber.complete();
        },
      });
    });
  }
}
