import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { CouponIssueWithBusiness } from '../../generated/coupon_stream';
import { PAGE_LIMIT } from 'src/types';

export function streamActiveCouponIssuesWithBusiness(db: Db): Observable<CouponIssueWithBusiness> {
  return new Observable(subscriber => {
    (async () => {
      try {
        const initialResults = await db.collection('couponIssues')
          .find({ status: { $in: ['active', 'suspended', 'ended'] } })
          .limit(PAGE_LIMIT)
          .toArray();

        for (const doc of initialResults) {
          const couponIssue: CouponIssueWithBusiness = {
            couponIssueId: doc._id.toString(),
            businessId: doc.businessId.toString(),
            couponName: doc.couponName || 'Default Coupon Name',
            businessName: '', 
            status: doc.status,
          };

          const business = await db.collection<any>('businesses').findOne({ _id: doc.businessId });
          if (business) {
            couponIssue.businessName = business.title.en;
          }

          subscriber.next(couponIssue);
        }

        const changeStream = db.collection('couponIssues').watch(
          [{ $match: { 'fullDocument.status': { $in: ['active', 'suspended', 'ended'] } } }],
          { fullDocument: 'updateLookup' }
        );

        changeStream.on('change', async (change: any) => {
          if (change.fullDocument) {
            const couponIssue: CouponIssueWithBusiness = {
              couponIssueId: change.fullDocument._id.toString(),
              businessId: change.fullDocument.businessId.toString(),
              couponName: change.fullDocument.couponName || 'Default Coupon Name',
              businessName: '',
              status: change.fullDocument.status,
            };

            try {
              const business = await db.collection<any>('businesses').findOne({ _id: change.fullDocument.businessId });
              if (business) {
                couponIssue.businessName = business.title.en;
              }

              subscriber.next(couponIssue);
            } catch (error) {
              subscriber.error(error);
            }
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
        subscriber.error(error);
      }
    })();
  });
}
