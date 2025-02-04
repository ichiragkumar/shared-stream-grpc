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
  LanguageFilter,
  WalletBalanceResponse,
  ActiveDrawnResponse,
  User
} from "../generated/coupon_stream"
import { Db } from 'mongodb';
import { DatabaseService } from 'src/config/database.config';
import { streamCouponIssues } from './streams/streamCouponIssue';
import { streamActiveCouponIssuesWithBusiness } from './streams/streamActiveCouponIssuesWithBusiness';
import { streamActiveBusinessesWithContractTypes } from './streams/streamActiveBusinessesWithContractTypes';
import {streamWalletBalance} from "./streams/streamWalletBalance"
import { streamActiveDrawn } from './streams/streamActiveDrawn';
import { streamMoreCouponRequestsService } from './streams/streamMoreCouponRequests';
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

  streamCouponIssuesService(data: LanguageFilter): Observable<CouponIssue> {
    return streamCouponIssues(data, this.db);
  }

  streamMoreCouponRequestsService(data: User): Observable<MoreCouponRequest> {
    return streamMoreCouponRequestsService( this.db, data);
  }

  

  streamActiveCouponIssuesWithBusinessService(data: LanguageFilter): Observable<CouponIssueWithBusiness> {
    return streamActiveCouponIssuesWithBusiness(this.db, data);
}


  streamActiveBusinessesWithContractTypesService(data: LanguageFilter): Observable<ActiveBusinessesStreamResponse> {
    return streamActiveBusinessesWithContractTypes(data, this.db);
  }

  streamActiveCouponsStreamService(data: User): Observable<ActiveCouponStreamResponse> {
    return streamActiveCouponsStream(this.db, data);
  }

  streamWalletService(data: User): Observable<WalletBalanceResponse > {
    return streamWalletBalance(this.db, data);
  }

  streamActiveDrawnService(data: LanguageFilter): Observable<ActiveDrawnResponse>{
    return streamActiveDrawn(this.db, data);
  }

  

}