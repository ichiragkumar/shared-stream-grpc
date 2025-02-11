
import { Document } from 'mongodb';
import { Balance } from './generated/coupon_stream';


export interface BusinessDocument extends Document {
  _id: string;
  description: Record<string, string>;
  title: Record<string, string>;
  logo: Record<string, Record<string, Record<string, string>>>;
  categories?: string[];
  suspended: boolean;
  sponsorshipType?: string | null;
  createdAt: Date;
}

export interface SpecialOfferDocument extends Document {
  _id: string;
  contractId: string
  businessId: string;
  status: string;
  title: Record<string, string>;
  image: Record<string, string>;
  startAt: Date;
  endAt: Date;
  displayedFrom: Date;
  displayedUntil: Date;
  content:Record<string, string>;
  zoneIds:string[]
  createdAt: Date;
  level: string;
  orderWeight:number;
  viewsCount:number;
}

export interface BusinessBranchDetailDocument extends Document {
  _id: string;
  images: string[];
  address:Record<string, string>;
  phone: string;
  title: Record<string, string>;
  _parentId: string;
  createdAt: Date;
}


export interface BusinessBranchDocument extends Document {
  _id: string;
  businessSuspended:boolean;
  shortAddress:Record<string, string>;
  businessId:string;
  zoneId:string;
  location:Record<string, number>;
  openingHours:Record<any, any>;
  createdAt:Date;
  contractTypes:Array<string>;
  _parentId:string;
}

export const PAGE_LIMIT = 10;


export enum STREAM_TYPE {
    BASE   =0,
    UPDATE =1,
    INSERT =2,
    DELETE =3,
    REMOVED = 4
}


export interface CouponIssueWithBusiness {
  couponIssueId: string;
  businessId: string;
  couponName: string;
  businessName: string;
  status: string;
  logo?: string;
  categories?: string[];
  title?: string;
  endsAt?: number;
  amountLeft?: number;
  type?: string;
  priceAmount?: number;
  currency?: string;
  drawId?: string;
  sellPriceAmount?: number;
  restrictedBranchIds?: string[];
  drawNumbers?: string[];
  descriptionFile?: string;
  purchasePriceAmount?: number;
  arrangement?: number;
}


export const safeParseDate = (dateValue: any): string => {
  if (!dateValue) return '';
  
  try {
    if (dateValue.$date) {
      if (typeof dateValue.$date === 'string') {
        return dateValue.$date;
      }
      if (typeof dateValue.$date === 'number') {
        return new Date(dateValue.$date).toISOString();
      }
    }
    
    if (typeof dateValue === 'string' || typeof dateValue === 'number') {
      const date = new Date(dateValue);
      if (!isNaN(date.getTime())) {
        return date.toISOString();
      }
    }
    
    return '';
  } catch (error) {
    return '';
  }
};