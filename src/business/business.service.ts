import { Injectable, BadRequestException } from '@nestjs/common';
import { Db, Filter } from 'mongodb';
import { DatabaseService } from 'src/config/database.config';
import { BusinessBranchDetailDocument, BusinessBranchDocument, BusinessDocument } from 'src/types';



@Injectable()
export class BusinessService {
  private db: Db;

  constructor() {}

  async onModuleInit() {
    this.db = await DatabaseService.connect();
  }

  async getBusinessById(id: string, languageCode: string = 'en', brightness: string = 'light') {
    if (!id) {
      throw new BadRequestException('Invalid request: id is missing.');
    }

    try {
      const filter: Filter<BusinessDocument> = { _id: id } as Filter<BusinessDocument>;
      const business = await this.db
        .collection<BusinessDocument>('businesses')
        .findOne(filter);

      if (!business) {
          return {
            message: `'No businesses found for the provided IDs.: getBusinessesWithIds'`,
          };
      }
  

      const localizedName = business.description?.[languageCode] || business.description?.['en'] || 'Unknown';
      const localizedTitle = business.title?.[languageCode] || business.title?.['en'] || 'Unknown';
      const image = business.logo?.[brightness]?.[languageCode] || business.logo?.[brightness]?.['en'] || null;

      return {
        id: business._id,
        name: localizedName,
        title: localizedTitle,
        image,
        categories: business.categories || [],
        createdAt: business.createdAt,
        suspended: business.suspended,
        sponsorshipType: business.sponsorshipType || null,
      };
    } catch (error) {
      return {
        message: `Internal Server Error :getBusinessesWithId`,
      }

    }
  }

  async getBusinessesWithIds(ids: string[], languageCode: string = 'en', brightness: string = 'light') {
    if (!ids || ids.length === 0) {
      throw new BadRequestException("'ids' must be a non-empty array. : getBusinessesWithIds");
    }

    try {
      const filter: Filter<BusinessDocument> = { 
        _id: { $in: ids } 
      } as Filter<BusinessDocument>;
      
      const businesses = await this.db
        .collection<BusinessDocument>('businesses')
        .find(filter)
        .toArray();

    
      if (!businesses.length) {
        return {
          message: `'No businesses found for the provided IDs.: getBusinessesWithIds'`,
        };
      }

      return businesses.map(business => ({
        id: business._id,
        name: business.description?.[languageCode] || business.description?.['en'] || 'Unknown',
        title: business.title?.[languageCode] || business.title?.['en'] || 'Unknown',
        image: business.logo?.[brightness]?.[languageCode] || business.logo?.[brightness]?.['en'] || null,
        categories: business.categories || [],
        createdAt: business.createdAt,
        suspended: business.suspended,
        sponsorshipType: business.sponsorshipType || null,
      }));
    } catch (error) {
      return {
        message: `Internal Server Error :getBusinessesWithIds`,
      }
    }
  }


  async getBusinessBranchDetails(
    businessId: string, 
    businessBranchId: string, 
    languageCode: string = 'en'
  ) {
    if (!businessId || !businessBranchId) {
      throw new BadRequestException('Invalid request: businessId or businessBranchId is missing.');
    }
  
    try {
      const [businessBranchDetail, businessBranch] = await Promise.all([
        this.db.collection<BusinessBranchDetailDocument>('businessBranchDetails').findOne({ _id: businessBranchId }),
        this.db.collection<BusinessBranchDocument>('businessBranches').findOne({ businessId: businessId }),
      ]);

      if (!businessBranch || !businessBranchDetail) {
        return {
          message: `Business branch with id ${businessBranchId} or ${businessId} not found.`,
          businessBranch: null,
          businessBranchDetail: null
        };
      }
  

      const localizedTitle = businessBranchDetail.title?.[languageCode] || businessBranchDetail.title?.['en'] || 'Unknown';
      const localizedShortAddress = businessBranch.shortAddress?.[languageCode] || businessBranch.shortAddress?.['en'] || 'Unknown';
  

      return {
        id: businessBranchDetail._id,
        title: localizedTitle,
        shortAddress: localizedShortAddress,
        address: businessBranchDetail.address?.[languageCode] || businessBranchDetail.address?.['en'],
        phone: businessBranchDetail.phone,
        images: businessBranchDetail.images,
        location: businessBranch.location,
        openingHours: businessBranch.openingHours,
      };
    } catch (error) {
      return {
        message: `Internal Server Error: getBusinessBranchDetails`,
      }
    }
  }
  
  

}