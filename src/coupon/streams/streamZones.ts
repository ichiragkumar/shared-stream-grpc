import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { LoggerService } from '@nestjs/common';
import { ZoneStreamResponse, UserPrefrences } from 'src/generated/coupon_stream';
import { DEFAUlT_SETTINGS } from 'src/config/constant';

export function streamZones(db: Db, userPrefrences: UserPrefrences,logger: LoggerService): Observable<ZoneStreamResponse> {
  return new Observable(subscriber => {
    const languageCode = userPrefrences?.languageCode || DEFAUlT_SETTINGS.LANGUAGE_CODE;



    const streamMetrics = {
      startTime: Date.now(),
      initialDocumentsCount: 0,
      changeEventsCount: 0,
      errors: 0,
    };

    logger.log('Stream initialization', {
      context: 'streamZones',
      languageCode
    });

    (async () => {
      try {
        const fetchStartTime = Date.now();


        const zoneDocuments = db.collection('zones')
          .find({});

        let hasZones = false;
        for await (const document of zoneDocuments) {
          hasZones = true;
          streamMetrics.initialDocumentsCount++;

          const zoneResponse = mapZoneResponse(document, languageCode);

          logger.log('Initial document emission', {
            context: 'streamZones',
            documentId: document._id,
            elapsedTime: Date.now() - fetchStartTime,
          });

          subscriber.next(zoneResponse);
        }

        if (!hasZones) {
          logger.warn('No zones found', {
            context: 'streamZones'
          });


          subscriber.next({
            id: 'No Zones Found',
            country: '',
            createdAt: '',
            isDefault: false,
            name: '',
            location: { latitude: 0, longitude: 0 },
            streamType: 0
          });
        }

        logger.log('Initial fetch completed', {
          context: 'streamZones',
          documentsProcessed: streamMetrics.initialDocumentsCount,
          fetchDuration: Date.now() - fetchStartTime,
          memoryUsage: process.memoryUsage(),
        });
      } catch (error) {
        streamMetrics.errors++;
        logger.error('Error fetching initial documents', {
          context: 'streamZones',
          error: {
            message: error.message,
            stack: error.stack,
          },
          metrics: {
            totalErrors: streamMetrics.errors,
          },
        });

        subscriber.error(error);
      }
    })();


    const changeStream = db.collection('zones').watch(
      [],
      { fullDocument: 'updateLookup' }
    );

    logger.log('Change stream established', {
      context: 'streamZones'
    });

    changeStream.on('change', (change: any) => {
      if (!change.fullDocument) {
        logger.warn('Change event without full document', {
          context: 'streamZones',
          operationType: change.operationType,
          documentId: change.documentKey?._id,
        });

        return;
      }

      streamMetrics.changeEventsCount++;
      logger.log('Change event processing', {
        context: 'streamZones',
        operationType: change.operationType,
        documentId: change.fullDocument._id,
        totalChanges: streamMetrics.changeEventsCount,
        timeSinceStart: Date.now() - streamMetrics.startTime,
      });

      subscriber.next(mapZoneResponse(change.fullDocument, languageCode));
    });

    changeStream.on('error', error => {
      streamMetrics.errors++;
      logger.error('Change stream error', {
        context: 'streamZones',
        error: {
          message: error.message,
          stack: error.stack,
        },
        metrics: {
          totalErrors: streamMetrics.errors,
          uptime: Date.now() - streamMetrics.startTime,
        },
      });
      subscriber.error(error);
    });

    subscriber.add(() => {
      logger.log('Stream cleanup', {
        context: 'streamZones',
        metrics: {
          duration: Date.now() - streamMetrics.startTime,
          initialDocuments: streamMetrics.initialDocumentsCount,
          changeEvents: streamMetrics.changeEventsCount,
          errors: streamMetrics.errors,
          memoryUsage: process.memoryUsage(),
        },
      });
      console.log('Cleaning up change stream');
      changeStream.close();
    });
  });
}

function mapZoneResponse(document: any, languageCode: string): ZoneStreamResponse {
  return {
    id: document._id?.toString() || '',
    country: document.country || '',
    createdAt: document.createdAt?.$date || document.createdAt || '',
    isDefault: document.default ?? false,
    name: document.name?.[languageCode] || document.name?.en || 'Unknown Name',
    location: {
      latitude: document.location?.latitude ?? 0,
      longitude: document.location?.longitude ?? 0,
    },
    streamType: 1,
  };
}
