import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { User, ActiveCouponStreamResponse } from '../../generated/coupon_stream';
import { USER_COUPON_STATUS } from 'src/config/constant';

export function streamActiveCouponsStream(db: Db, data: User): Observable<ActiveCouponStreamResponse> {
  return new Observable(subscriber => {
    const { userId } = data;

    (async () => {
      try {

        const filterCondition = {
          userId, 
          status: { $in: ['active', 'suspended', 'ended'] }, 
          redeemedBySelfActivation: false 
        };


        const userExists = await db.collection('userCoupons').findOne(filterCondition);
        if (!userExists) {
          subscriber.next({
            Id: 'User ID Does not Exist',
            status: 'No matching coupons',
            redemptionInfo:{
              redeemedByBusinessManagerId: '',
              methodOfRedemption: ''
            },
            code: '',
            businessId: '',
            couponIssueId: '',
            redeemedBySelfActivation: false,
            purchasePrice: 0,
            purchaseCurrency: '',
            userId,
            expireAt:"",
            createdAt:"",
            purchasedAt:""
          });
          subscriber.complete();
          return;
        }


        const initialDocuments = db.collection('userCoupons').find(filterCondition);
        for await (const doc of initialDocuments) {
          subscriber.next(mapToCouponIssue(doc));
        }


        const changeStream = db.collection('userCoupons').watch(
          [
            {
              $match: {
                'fullDocument.userId': userId,
                'fullDocument.status': { $in: USER_COUPON_STATUS },
                'fullDocument.redeemedBySelfActivation': false
              }
            }
          ],
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


        subscriber.add(() => {
          console.log('Cleaning up change stream');
          changeStream.close();
        });

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
    expireAt: doc.expireAt,
    createdAt: doc.createdAt,
    purchasedAt: doc.purchasedAt
  };
}
