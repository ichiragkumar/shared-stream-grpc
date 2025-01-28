import { Injectable, NotFoundException } from '@nestjs/common';
import { Observable } from 'rxjs';
import { 
  StatusFilter, 
  CouponIssue, 
  UserFilter,
  MoreCouponRequest,
  CouponStatusFilter,
  UserCoupon,
  CouponIssueWithBusiness,
  ActiveBusinessesStreamResponse,
  ActiveCouponStreamResponse
} from "../generated/coupon_stream"
import { Db, Filter, ObjectId } from 'mongodb';
import { DatabaseService } from 'src/config/database.config';

interface ActiveCouponIssueWithBusiness {
    id: string;
    status: string;
    businessId: string;
    updatedAt: number;
    businessName?: string;
    businessLogo?: string;
    couponIssueId: string;
    couponName: string;
}

  

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


  streamActiveCouponIssuesWithBusiness(): Observable<CouponIssueWithBusiness> {
    return new Observable(subscriber => {
      const changeStream = this.db.collection('couponIssues').watch(
        [
          { 
            $match: {
              'fullDocument.status': { $in: ['active', 'suspended', 'ended'] },
            },
          },
        ],
        { fullDocument: 'updateLookup' }
      );
  
      changeStream.on('change', async (change: any) => {
        if (change.fullDocument) {
          const couponIssue: any = {
            id: change.fullDocument._id.toString(),
            status: change.fullDocument.status,
            businessId: change.fullDocument.businessId,
            updatedAt: Date.now(),
            couponIssueId: change.fullDocument._id.toString(), 
            couponName: change.fullDocument.couponName || 'Default Coupon Name',
          };
  
          try {
            const filter: Filter<any> = { _id:couponIssue.businessId } as Filter<any>;
                  const business = await this.db
                    .collection<any>('businesses')
                    .findOne(filter);
            if (!business) {
                    throw new NotFoundException(`Business with id ${couponIssue.businessId} not found.`);
            }
  
            if (business) {
              couponIssue.businessName = business.title.en; 
              couponIssue.businessLogo = business.logo.light.en;
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
    });
  }

  streamActiveBusinessesWithContractTypes(): Observable<ActiveBusinessesStreamResponse> {
    return new Observable(subscriber => {
      const changeStream = this.db.collection('businesses').watch(
        [
          {
            $match: {
              'fullDocument.contractTypes': { $in: ['vendor', 'advertiser', 'sponsor', 'specialIssue', 'business', 'voucher'] },
            },
          },
        ],
        { fullDocument: 'updateLookup' }
      );

      changeStream.on('change', async (change: any) => {
        if (change.fullDocument) {
          const business = change.fullDocument;

          const response: ActiveBusinessesStreamResponse = {
            id: business._id.toString(),
            title: business.title?.en || 'Unknown Title',
            description: business.description?.en || 'No Description',
            image: business.logo?.light?.en || null,
            categories: business.categories || [],
            businessId: business._id.toString(),
            contractType: business.contractTypes.join(', '),
          };

          subscriber.next(response);
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


  streamActiveCouponsStream(data: UserFilter): Observable<ActiveCouponStreamResponse> {
    return new Observable(subscriber => {
        if (!data || !data.userId) {
            subscriber.error(new Error('Invalid request: statuses are required.'));
            return;
        }

      const { userId } = data;
      const userExists = this.db.collection('userCoupons').findOne({ userId });
      if (!userExists) {
        subscriber.error(new Error(`No coupon requests found for userId: ${userId}`));
        return;
      }

      const changeStream = this.db.collection('userCoupons').watch(
        [
          {
            $match: {
              'fullDocument.status': { $in: ['active', 'suspended', 'ended'] },
              'fullDocument.userId': userId
            },
          },
        ],
        { fullDocument: 'updateLookup' }
      );

      changeStream.on('change', async (change: any) => {
        if (change.fullDocument) {
          if (!change.fullDocument._id || !change.fullDocument.userId) {
            subscriber.error(new Error('Invalid document data received'));
            return;
          }

          const couponIssue: ActiveCouponStreamResponse = {
            _id: change.fullDocument._id,
            redemptionInfo: change.fullDocument.redemptionInfo || null,
            code: change.fullDocument.code,
            businessId: change.fullDocument.businessId,
            couponIssueId: change.fullDocument._id.toString(),
            redeemedBySelfActivation: change.fullDocument.redeemedBySelfActivation,
            purchasePrice: change.fullDocument.purchasePrice,            
            purchaseCurrency: change.fullDocument.purchaseCurrency,
            userId: change.fullDocument.userId,
            status: change.fullDocument.status,
            expireAt: change.fullDocument.expireAt,
            createdAt: change.fullDocument.createdAt,
            purchasedAt: change.fullDocument.purchasedAt,
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
  
  
  
}