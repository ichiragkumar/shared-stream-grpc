import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { CouponIssue, UserPrefrences } from '../../generated/coupon_stream';
import { STREAM_TYPE } from 'src/types';
import { DEFAUlT_SETTINGS, VALID_STATUS } from 'src/config/constant';

export function streamCouponIssues(
  userPrefrences: UserPrefrences,
  db: Db
): Observable<CouponIssue> {
  return new Observable((subscriber) => {
    const languageCode = userPrefrences?.languageCode || DEFAUlT_SETTINGS.LANGUAGE_CODE;
    const brightness = userPrefrences?.brightness || DEFAUlT_SETTINGS.BRIGHTNESS;

    (async () => {
      try {

        const initialDocuments = db
          .collection('couponIssues')
          .find({ status: { $in: VALID_STATUS } });

        for await (const doc of initialDocuments) {
          subscriber.next(mapCouponIssue(doc, languageCode, brightness, STREAM_TYPE.BASE));
        }


        const changeStream = db.collection('couponIssues').watch(
          [
            {
              $match: {
                'fullDocument.status': { $in: ['active', 'suspended', 'ended'] },
              },
            },
          ],
          { fullDocument: 'updateLookup' }
        );

        changeStream.on('change', (change: any) => {
          if (!change.fullDocument) return;

          let streamType: STREAM_TYPE;
          switch (change.operationType) {
            case 'insert':
              streamType = STREAM_TYPE.INSERT;
              break;
            case 'update':
              streamType = STREAM_TYPE.UPDATE;
              break;
            default:
              return;
          }

          subscriber.next(mapCouponIssue(change.fullDocument, languageCode, brightness, streamType));
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
