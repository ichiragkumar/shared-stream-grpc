import { Injectable, NotFoundException } from '@nestjs/common';
import { Observable } from 'rxjs';
import { 
  StatusFilter, 
  CouponIssue, 
  UserFilter,
  MoreCouponRequest,
  CouponIssueWithBusiness,
  ActiveBusinessesStreamResponse,
  ActiveCouponStreamResponse,
  Balance,
  LanguageFilter
} from "../generated/coupon_stream"
import { Db } from 'mongodb';
import { DatabaseService } from 'src/config/database.config';
import { streamCouponIssues } from './streams/streamCouponIssue';
import { streamMoreCouponRequestsService } from './streams/streamMoreCouponRequests';
import { streamActiveCouponIssuesWithBusiness } from './streams/streamActiveCouponIssuesWithBusiness';
import { streamActiveBusinessesWithContractTypes } from './streams/streamActiveBusinessesWithContractTypes';
import { streamActiveCouponsStream } from './streams/streamActiveCouponsStream';
import {streamWalletBalance} from "./streams/streamWalletBalance"



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

  

  streamActiveCouponIssuesWithBusinessService(data: LanguageFilter): Observable<CouponIssueWithBusiness> {
    return streamActiveCouponIssuesWithBusiness(this.db, data);
}


  streamActiveBusinessesWithContractTypesService(): Observable<ActiveBusinessesStreamResponse> {
    return streamActiveBusinessesWithContractTypes(this.db);
  }

  streamActiveCouponsStreamService(data: StatusFilter): Observable<ActiveCouponStreamResponse> {
    return streamActiveCouponsStream(this.db, data);
  }


  
  streamWalletService(data: UserFilter): Observable<Balance > {
    return streamWalletBalance(this.db, data);
  }

}