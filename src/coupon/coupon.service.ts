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
import { Db } from 'mongodb';
import { DatabaseService } from 'src/config/database.config';
import { PAGE_LIMIT } from 'src/types';
import { streamCouponIssues } from './streams/streamCouponIssue';
import { streamMoreCouponRequestsService } from './streams/streamMoreCouponRequests';
import { streamActiveCouponIssuesWithBusiness } from './streams/streamActiveCouponIssuesWithBusiness';
import { streamActiveBusinessesWithContractTypes } from './streams/streamActiveBusinessesWithContractTypes';
import { streamActiveCouponsStream } from './streams/streamActiveCouponsStream';

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

  streamCouponIssuesService(data: StatusFilter): Observable<CouponIssue> {
    return streamCouponIssues(data, this.db);
  }


  streamMoreCouponRequestsService(data: UserFilter): Observable<MoreCouponRequest> {
    return streamMoreCouponRequestsService(data, this.db);
  }

  streamActiveCouponIssuesWithBusinessService(): Observable<CouponIssueWithBusiness> {
    return streamActiveCouponIssuesWithBusiness(this.db);
  }

  getCouponsByStatus(data: CouponStatusFilter): Observable<UserCoupon> {
    return new Observable(subscriber => {
      // Add your streaming implementation
    });
  }



  streamActiveBusinessesWithContractTypesService(): Observable<ActiveBusinessesStreamResponse> {
    return streamActiveBusinessesWithContractTypes(this.db);
  }

  streamActiveCouponsStreamService(data: UserFilter): Observable<ActiveCouponStreamResponse> {
    return streamActiveCouponsStream(this.db, data);
  }



  
  
}