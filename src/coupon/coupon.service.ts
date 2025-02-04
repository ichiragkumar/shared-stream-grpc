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
  ActiveDrawnResponse
} from "../generated/coupon_stream"
import { Db } from 'mongodb';
import { DatabaseService } from 'src/config/database.config';
import { streamCouponIssues } from './streams/streamCouponIssue';
import { streamMoreCouponRequestsService } from './streams/streamMoreCouponRequests';
import { streamActiveCouponIssuesWithBusiness } from './streams/streamActiveCouponIssuesWithBusiness';
import { streamActiveBusinessesWithContractTypes } from './streams/streamActiveBusinessesWithContractTypes';
import { streamActiveCouponsStream } from './streams/streamActiveCouponsStream';
import {streamWalletBalance} from "./streams/streamWalletBalance"
import { streamActiveDrawn } from './streams/streamActiveDrawn';



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

  streamMoreCouponRequestsService(data: UserFilter): Observable<MoreCouponRequest> {
    return streamMoreCouponRequestsService(data, this.db);
  }

  

  streamActiveCouponIssuesWithBusinessService(data: LanguageFilter): Observable<CouponIssueWithBusiness> {
    return streamActiveCouponIssuesWithBusiness(this.db, data);
}


  streamActiveBusinessesWithContractTypesService(data: LanguageFilter): Observable<ActiveBusinessesStreamResponse> {
    return streamActiveBusinessesWithContractTypes(data, this.db);
  }

  streamActiveCouponsStreamService(data: StatusFilter): Observable<ActiveCouponStreamResponse> {
    return streamActiveCouponsStream(this.db, data);
  }

  streamWalletService(data: UserFilter): Observable<WalletBalanceResponse > {
    return streamWalletBalance(this.db, data);
  }

  streamActiveDrawnService(data: LanguageFilter): Observable<ActiveDrawnResponse>{
    return streamActiveDrawn(this.db, data);
  }

  

}