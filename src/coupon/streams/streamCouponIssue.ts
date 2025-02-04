import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { CouponIssue, LanguageFilter } from '../../generated/coupon_stream';
import { STREAM_TYPE } from 'src/types';

export function streamCouponIssues(
  languageFilter: LanguageFilter,
  db: Db
): Observable<CouponIssue> {
  return new Observable((subscriber) => {
    const languageCode = languageFilter?.languageCode || 'en';


    db.collection('couponIssues')
      .find({ status: { $in: ['active', 'suspended', 'ended'] } })
      .forEach((doc) => {
        const couponIssue = mapCouponIssue(doc, languageCode, STREAM_TYPE.BASE);
        subscriber.next(couponIssue);
      })
      .then(() => {
        const changeStream = db.collection('couponIssues').watch(
          [
            {$match: { }},
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

          const couponIssue = mapCouponIssue(
            change.fullDocument,
            languageCode,
            streamType
          );
          subscriber.next(couponIssue);
        });

        changeStream.on('error', (error: any) => {
          console.error('Change stream error:', error);
          subscriber.error(error);
        });

        return () => {
          console.log('Cleaning up change stream');
          changeStream.close();
        };
      })
      .catch((error) => {
        console.error('Error fetching initial data:', error);
        subscriber.error(error);
      });
  });
}

function mapCouponIssue(doc: any, languageCode: string, streamType: STREAM_TYPE): CouponIssue {
  return {
    Id: doc._id.toString(),
    drawId: doc.drawId,
    businessContractId: doc.businessContractId,
    deliveryAvailable: doc.deliveryAvailable,
    deliveryContactPhone: doc.deliveryContactPhone,


    title: doc.title?.[languageCode] || doc.title?.['en'] || 'Unknown',
    image: doc.image?.light?.[languageCode] || doc.image?.light?.['en'] || '',
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
    id:doc.id,
    restrictions: doc.restrictions?.[languageCode] || doc.restrictions?.['en'] || '',
    methodsOfRedemption: doc.methodsOfRedemption,
    amountUsed: doc.amountUsed,
    amountSold: doc.amountSold,
    streamtype: streamType,
  };
}
