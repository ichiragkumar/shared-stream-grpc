import { Injectable } from '@nestjs/common';
import { Observable } from 'rxjs';
import { 
  StatusFilter, 
  CouponIssue, 
  ExpiredCouponFilter, 
  ExpiredCouponIssue,
  UserFilter,
  MoreCouponRequest,
  CouponStatusFilter,
  UserCoupon
} from "../generated/coupon_stream"
import { Db } from 'mongodb';
import { DatabaseService } from 'src/config/database.config';

@Injectable()
export class CouponService {
  private db: Db;

  constructor() {}

  async onModuleInit() {
    this.db = await DatabaseService.connect();

  }

  streamCouponIssues(data: StatusFilter): Observable<CouponIssue> {
    return new Observable(subscriber => {
      if (!data.statuses || data.statuses.length === 0) {
        subscriber.error(new Error('Invalid request: statuses are required.'));
        return;
      }

      const changeStream = this.db.collection('couponIssues').watch(
        [
          { $match: { 'fullDocument.status': { $in: data.statuses } } }
        ],
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

  streamExpiredCoupons(data: ExpiredCouponFilter): Observable<ExpiredCouponIssue> {
    return new Observable(subscriber => {
      // Add your streaming implementation
    });
  }

  streamMoreCouponRequests(data: UserFilter): Observable<MoreCouponRequest> {
    return new Observable<MoreCouponRequest>( subscriber=> {
      const { userId } = data;

      if (!userId) {
        subscriber.error(new Error('Invalid request: userId is required.'));
        return;
      }

      const userExists =  this.db.collection('moreCouponsRequests').findOne({ userId });
      if (!userExists) {
        subscriber.error(new Error(`No more coupon requests found for the given userId: ${userId}`));
        return;
      }


      const changeStream = this.db.collection('moreCouponsRequests').watch(
        [
          {
            $match: {
              'fullDocument.userId': userId,
            },
          },
        ],
        { fullDocument: 'updateLookup' }
      );

      changeStream.on('change', (change: any) => {
        if (change.fullDocument) {
          const { _id, userId, couponIssueId, createdAt, _parentId } = change.fullDocument;

          const moreCouponRequest: MoreCouponRequest = {
            id: _id.toString(),
            userId,
            couponIssueId,
            createdAt: new Date(createdAt).getTime(),
            parentId: _parentId,
          };

          subscriber.next(moreCouponRequest);
        }
      });

      changeStream.on('error', (err: any) => {
        console.error('Change stream error:', err);
        subscriber.error(new Error('An error occurred while streaming changes.'));
      });

      return () => {
        console.log('Cleaning up change stream');
        changeStream.close();
      };
    });
  }

  getCouponsByStatus(data: CouponStatusFilter): Observable<UserCoupon> {
    return new Observable(subscriber => {
      // Add your streaming implementation
    });
  }
}