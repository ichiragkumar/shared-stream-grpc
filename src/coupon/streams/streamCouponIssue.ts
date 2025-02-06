import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { CouponIssue, UserPrefrences } from '../../generated/coupon_stream';
import { STREAM_TYPE } from 'src/types';
import { DEFAUlT_SETTINGS } from 'src/config/constant';
import { TRACKED_STATUS, NOT_TRACKED_STATUS } from 'src/config/constant';

export function streamCouponIssues(
  userPrefrences: UserPrefrences,
  db: Db
): Observable<CouponIssue> {
  return new Observable((subscriber) => {
    const languageCode = userPrefrences?.languageCode || DEFAUlT_SETTINGS.LANGUAGE_CODE;
    const brightness = userPrefrences?.brightness || DEFAUlT_SETTINGS.BRIGHTNESS;

    (async () => {
      try {
        const issueCouponDocuments = db.collection('couponIssues').find({ status: { $in: TRACKED_STATUS } });

        for await (const document of issueCouponDocuments) {
          subscriber.next(mapCouponIssue(document, languageCode, brightness, STREAM_TYPE.BASE));
        }

        await issueCouponDocuments.close();
        const changeStream = db.collection('couponIssues').watch([], { fullDocument: 'updateLookup' });

        changeStream.on('change', (change: any) => {
          if (!change.fullDocument) return;

          let streamType: STREAM_TYPE;
          let mappedIssue: CouponIssue | null = null;

          const previousStatus = change?.updateDescription?.removedFields?.includes('status')
            ? null
            : change?.updateDescription?.updatedFields?.status;

          const newStatus = change.fullDocument?.status;

          const wasTracked = previousStatus && TRACKED_STATUS.includes(previousStatus);
          const isNowTracked = TRACKED_STATUS.includes(newStatus);
          const isNowNotTracked = NOT_TRACKED_STATUS.includes(newStatus);

          switch (change.operationType) {
            case 'update':
              if (wasTracked && isNowTracked) {
                streamType = STREAM_TYPE.UPDATE;
                mappedIssue = mapCouponIssue(change.fullDocument, languageCode, brightness, streamType);
              } else if (wasTracked && isNowNotTracked) {
                streamType = STREAM_TYPE.REMOVED;
                mappedIssue = mapCouponIssue(change.fullDocument, languageCode, brightness, streamType);
              } else if (!wasTracked && isNowTracked) {
                streamType = STREAM_TYPE.INSERT;
                mappedIssue = mapCouponIssue(change.fullDocument, languageCode, brightness, streamType);
              } else if (!wasTracked && isNowNotTracked) {
                streamType = STREAM_TYPE.REMOVED;
                mappedIssue = mapCouponIssue(change.fullDocument, languageCode, brightness, streamType);
              }
              break;
          }

          if (mappedIssue) {
            subscriber.next(mappedIssue);
          }
        });

        changeStream.on('error', (error: any) => {
          console.error('Change stream error:', error);
          subscriber.error(error);
        });

        subscriber.add(() => {
          console.log('Cleaning up change stream');
          changeStream.close();
        });
      } catch (error) {
        console.error('Error fetching or streaming data:', error);
        subscriber.error(error);
      }
    })();
  });
}

function mapCouponIssue(doc: any, languageCode: string, brightness: string, streamType: STREAM_TYPE): CouponIssue {
  return {
    id: doc._id ? doc._id.toString() : '',
    drawId: doc.drawId,
    businessContractId: doc.businessContractId,
    deliveryAvailable: doc.deliveryAvailable,
    deliveryContactPhone: doc.deliveryContactPhone,
    title: doc.title?.[languageCode] || doc.title?.['en'] || 'Unknown',
    image: doc.image?.[brightness]?.[languageCode] || doc.image?.[brightness]?.['en'] || '',
    descriptionFile: doc.descriptionFile?.[languageCode] || doc.descriptionFile?.['en'] || '',
    activeAt: doc.activeAt,
    endAt: doc.endAt,
    expireAt: doc.expireAt,
    zoneIds: doc.zoneIds,
    initialAmount: doc.initialAmount,
    currency: doc.currency,
    purchasePriceAmount: doc.purchasePriceAmount,
    discountAmount: doc.discountAmount,
    sellPriceAmount: doc.sellPriceAmount,
    ticketPriceAmount: doc.ticketPriceAmount,
    grandDrawMultiplier: doc.grandDrawMultiplier,
    couponsSource: doc.couponsSource,
    couponsCsvPath: doc.couponsCsvPath,
    additionalCouponsCsvPath: doc.additionalCouponsCsvPath,
    arrangement: doc.arrangement,
    couponsPrefix: doc.couponsPrefix,
    businessId: doc.businessId,
    status: doc.status,
    createdAt: doc.createdAt,
    type: doc.type,
    amountExpired: doc.amountExpired,
    additionalAmount: doc.additionalAmount,
    lastIncrId: doc.lastIncrId,
    nextCodeIncrId: doc.nextCodeIncrId,
    RawPath: doc._rawPath,
    restrictions: doc.restrictions?.[languageCode] || doc.restrictions?.['en'] || '',
    methodsOfRedemption: doc.methodsOfRedemption,
    amountUsed: doc.amountUsed,
    amountSold: doc.amountSold,
    streamtype: streamType,
  };
}
