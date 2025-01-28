
import { Document } from 'mongodb';


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
