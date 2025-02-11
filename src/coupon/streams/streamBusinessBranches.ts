import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { LoggerService } from '@nestjs/common';
import { BusinessBranchStreamResponse, UserPrefrences } from 'src/generated/coupon_stream';
import { DEFAUlT_SETTINGS } from 'src/config/constant';



export function streamBusinessBranches(
  db: Db, 
  userPrefrences: UserPrefrences, 
  logger: LoggerService
): Observable<BusinessBranchStreamResponse> {
  return new Observable(subscriber => {
    const languageCode = userPrefrences?.languageCode || DEFAUlT_SETTINGS.LANGUAGE_CODE;

    const streamMetrics = {
      startTime: Date.now(),
      initialDocumentsCount: 0,
      changeEventsCount: 0,
      errors: 0,
    };

    logger.log('Stream initialization', {
      context: 'streamBusinessBranches',
      languageCode
    });

    (async () => {
      try {
        const fetchStartTime = Date.now();


        const cursor = db.collection('businessBranches').aggregate([
          {
            $lookup: {
              from: 'businesses',
              localField: 'businessId',
              foreignField: '_id',
              as: 'businessDetails'
            }
          },
          { $unwind: { path: "$businessDetails", preserveNullAndEmptyArrays: true } },
          
        ]);

        let hasBranches = false;
        for await (const document of cursor) {
          hasBranches = true;
          streamMetrics.initialDocumentsCount++;

          const branchResponse = mapBusinessBranchResponse(document, languageCode,0);

          logger.log('Initial document emission', {
            context: 'streamBusinessBranches',
            documentId: document._id,
            elapsedTime: Date.now() - fetchStartTime,
          });

          subscriber.next(branchResponse);
        }

        if (!hasBranches) {
          logger.warn('No business branches found', {
            context: 'streamBusinessBranches'
          });

          subscriber.next({
            id: 'No Branches Found',
            businessSuspended: false,
            shortAddress: '',
            businessId: '',
            zoneId: '',
            location: { latitude: 0, longitude: 0 },
            openingHours:{},
            createdAt: '',
            contractTypes: [],
            streamType: 0,
          });
        }

        logger.log('Initial fetch completed', {
          context: 'streamBusinessBranches',
          documentsProcessed: streamMetrics.initialDocumentsCount,
          fetchDuration: Date.now() - fetchStartTime,
          memoryUsage: process.memoryUsage(),
        });
      } catch (error) {
        streamMetrics.errors++;
        logger.error('Error fetching initial documents', {
          context: 'streamBusinessBranches',
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


    const changeStream = db.collection('businessBranches').watch(
      [],
      { fullDocument: 'updateLookup' }
    );

    logger.log('Change stream established', {
      context: 'streamBusinessBranches'
    });

    changeStream.on('change', async (change: any) => {
      if (!change.fullDocument) {
        logger.warn('Change event without full document', {
          context: 'streamBusinessBranches',
          operationType: change.operationType,
          documentId: change.documentKey?._id,
        });

        return;
      }

      streamMetrics.changeEventsCount++;


      const businessDetails = await db.collection('businesses').findOne(
        { _id: change.fullDocument.businessId },
        { projection: { contractTypes: 1 } }
      );

      const updatedBranch = {
        ...change.fullDocument,
        contractTypes: businessDetails?.contractTypes || [],
      };

      logger.log('Change event processing', {
        context: 'streamBusinessBranches',
        operationType: change.operationType,
        documentId: change.fullDocument._id,
        totalChanges: streamMetrics.changeEventsCount,
        timeSinceStart: Date.now() - streamMetrics.startTime,
      });
      subscriber.next(mapBusinessBranchResponse(updatedBranch, languageCode,1));
    });

    changeStream.on('error', error => {
      streamMetrics.errors++;
      logger.error('Change stream error', {
        context: 'streamBusinessBranches',
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
        context: 'streamBusinessBranches',
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

function mapBusinessBranchResponse(document: any, languageCode: string, streamType: number): BusinessBranchStreamResponse {
    return {
      id: document._id?.toString() || '',
      businessSuspended: document.businessSuspended ?? false,
      shortAddress: document.shortAddress?.[languageCode] || document.shortAddress?.en || 'Unknown Address',
      businessId: document.businessId || '',
      zoneId: document.zoneId || '',
      location: {
        latitude: document.location?.latitude ?? 0,
        longitude: document.location?.longitude ?? 0,
      },
      openingHours: Object.fromEntries(
        Object.entries(document.openingHours || {}).map(([day, hours]: [string, any]) => [
          day,
          {
            openTime: hours?.openTime ?? '', 
            closeTime: hours?.closeTime ?? '', 
          }
        ])
      ),
      createdAt: document.createdAt?.$date ? new Date(document.createdAt.$date).toISOString() : document.createdAt || '',
      contractTypes: document.contractTypes || [],
      streamType: streamType
    };
  }