import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { User, ActiveCouponStreamResponse } from '../../generated/coupon_stream';

export function streamActiveCouponsStream(db: Db, data: User): Observable<ActiveCouponStreamResponse> {
  return new Observable(subscriber => {
    const { userId } = data;

    (async () => {
      try {
        const userExists = await db.collection('userCoupons').findOne({ userId });

        if (!userExists) {
          console.log('User does not exist');
          subscriber.error(new Error('User does not exist'));
          return;
        }


        const initialDocuments = db.collection('userCoupons').find({ userId });
        for await (const doc of initialDocuments) {
          subscriber.next(mapToCouponIssue(doc));
        }


        const changeStream = db.collection('userCoupons').watch(
          [{ $match: { 'fullDocument.userId': userId } }],
          { fullDocument: 'updateLookup' }
        );

        changeStream.on('change', (change: any) => {
          if (change.fullDocument) {
            subscriber.next(mapToCouponIssue(change.fullDocument));
          }
        });

        changeStream.on('error', (error: any) => {
          console.error('Change stream error:', error);
          subscriber.error(error);
        });

        return () => {
          console.log('Cleaning up change stream');
          changeStream.close();
        };

      } catch (error) {
        console.error('Error in streaming:', error);
        subscriber.error(error);
      }
    })();
  });
}


function mapToCouponIssue(doc: any): ActiveCouponStreamResponse {
  return {
    Id: doc._id?.toString(),
    redemptionInfo: doc.redemptionInfo || null,
    code: doc.code,
    businessId: doc.businessId,
    couponIssueId: doc._id?.toString(),
    redeemedBySelfActivation: doc.redeemedBySelfActivation,
    purchasePrice: doc.purchasePrice,
    purchaseCurrency: doc.purchaseCurrency,
    userId: doc.userId,
    status: doc.status,
    expireAt: { seconds: doc.expireAt.getTime() / 1000, nanos: 0 },
    createdAt: { seconds: doc.createdAt.getTime() / 1000, nanos: 0 }, 
    purchasedAt: { seconds: doc.purchasedAt.getTime() / 1000, nanos: 0 }
  };
}
