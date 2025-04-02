import { Observable } from 'rxjs';
import { Db, ChangeStream } from 'mongodb';
import { LoggerService } from '@nestjs/common';

export function streamEnvironment(db: Db, logger: LoggerService): Observable<any> {
  return new Observable((subscriber) => {
    logger.log('Fetching initial environment data...');
    const environmentDocuments = db.collection('environment');

    environmentDocuments.find({}).toArray()
      .then((documents) => {
        if (documents.length > 0) {
          documents.forEach((doc) => subscriber.next(doc));
          logger.log(`Emitted ${documents.length} initial environment records.`);
        } else {
          subscriber.next({ message: 'No environment data found' });
        }
      })
      .catch((error) => {
        logger.error('Error fetching initial environment data:', error);
        subscriber.error(error);
      });


    const changeStream: ChangeStream = environmentDocuments.watch([], { fullDocument: 'updateLookup' });

    changeStream.on('change', (change: any) => {
      logger.log(`Change detected: ${change.operationType}`);
      if (change.fullDocument) {
        subscriber.next(change.fullDocument);
      } else {
        logger.warn('Received change event but fullDocument is missing:', change);
      }
    });

    changeStream.on('error', (error) => {
      logger.error('Error in change stream:', error);
      subscriber.error(error);
    });

    return () => {
      logger.log('Closing environment change stream...');
      changeStream.close();
    };
  });
}
