
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
    DELETE =3
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
    // Handle MongoDB extended JSON date format
    if (dateValue.$date) {
      // If it's already an ISO string
      if (typeof dateValue.$date === 'string') {
        return dateValue.$date;
      }
      // If it's a number (timestamp)
      if (typeof dateValue.$date === 'number') {
        return new Date(dateValue.$date).toISOString();
      }
    }
    
    // Handle regular date strings or timestamps
    if (typeof dateValue === 'string' || typeof dateValue === 'number') {
      const date = new Date(dateValue);
      // Validate the date is valid before converting to ISO string
      if (!isNaN(date.getTime())) {
        return date.toISOString();
      }
    }
    
    return '';
  } catch (error) {
    console.error('Error parsing date:', error, 'Value:', dateValue);
    return '';
  }
};