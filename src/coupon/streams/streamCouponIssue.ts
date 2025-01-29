
import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { CouponIssue,  StatusFilter } from '../../generated/coupon_stream';


export function streamCouponIssues(data: StatusFilter, db: Db): Observable<CouponIssue> {
    return new Observable(subscriber => {
      const changeStream = db.collection('couponIssues').watch(
        [{ $match: { 'fullDocument.status': { $in: data.statuses } } }],
        { fullDocument: 'updateLookup' }
      );
  
      changeStream.on('change', (change: any) => {
        if (change.fullDocument) {
          const couponIssue: CouponIssue = {
            id: change.fullDocument._id.toString(),
            status: change.fullDocument.status,
            updatedAt: Date.now()
          };
          subscriber.next(couponIssue);
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
    });
  }