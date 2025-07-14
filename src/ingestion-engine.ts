import * as fs from 'fs';
import * as crypto from 'crypto';
import * as readline from 'readline';
import { Database } from './database.js';
import { Integration, FieldMap, OrderData, OrderStatus } from './types.js';
import { DataTransforms } from './transforms.js';
import { ProcessingError, ValidationError, Validators, Logger } from './utils.js';

export class IngestionEngine {
  constructor(private db: Database) {}

  async processFile(path: string, integrationKey?: string): Promise<void> {
    try {
      if (!Validators.isValidFilePath(path)) {
        throw new ValidationError('Invalid file path', { path });
      }

      if (integrationKey && !Validators.isValidIntegrationKey(integrationKey)) {
        throw new ValidationError('Invalid integration key', { integrationKey });
      }

      Logger.info(`Processing file: ${path}`);
      
      const hash = await this.hash(path);
      const result = await this.stream(path, hash, integrationKey);
      
      Logger.info(`Processed ${result.processed} records, skipped ${result.skipped}`);
      Logger.success('File processing completed successfully');
    } catch (error: unknown) {
      if (error instanceof ValidationError || error instanceof ProcessingError) {
        throw error;
      }
      throw new ProcessingError('Failed to process file', { originalError: error, path, integrationKey });
    }
  }

  private async stream(path: string, hash: string, integrationKey?: string) {
    const fileStream = fs.createReadStream(path, { encoding: 'utf-8' });
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity
    });

    let headers: string[] = [];
    let integration: Integration | null = null;
    let jobId: number | null = null;
    let processed = 0, skipped = 0, lineCount = 0;

    for await (const line of rl) {
      lineCount++;
      
      if (lineCount === 1) {
        // Parse headers
        headers = this.parseCSVLine(line).map(h => h.trim());
        
        integration = integrationKey 
          ? await this.db.getIntegrationByName(integrationKey)
          : await this.db.getIntegrationByHeaders(headers);
        
        if (!integration) {
          throw new ValidationError(
            integrationKey ? `Integration not found: ${integrationKey}` : 'Could not detect integration',
            { integrationKey, headers }
          );
        }
        
        Logger.info(`Using integration: ${integration.name}`);
        
        if (await this.db.isFileProcessed(integration.id!, "", hash)) {
          Logger.warn(`File already processed: ${path}`);
          return { processed: 0, skipped: 0 };
        }
        
        jobId = await this.db.createJob(integration.id!, path, 0);
        continue;
      }

      // Parse data line
      const fields = this.parseCSVLine(line);
      const record: Record<string, string> = {};
      headers.forEach((h, i) => record[h] = fields[i] || '');

      const transformed = this.transform(record, integration!);
      if (transformed) {
        await this.processRecord(transformed, integration!);
        processed++;
        if (processed % 100 === 0) Logger.info(`${processed} records processed...`);
      } else {
        skipped++;
      }
    }

    if (jobId && integration) {
      await this.db.updateJob(jobId, { 
        status: 'completed',
        totalRows: lineCount - 1,
        processedRows: processed,
        insertedRows: processed,
        errorRows: skipped
      });
      await this.db.recordProcessedFile(integration.id!, path, hash, lineCount - 1, jobId);
    }

    return { processed, skipped };
  }

  private parseCSVLine(line: string): string[] {
    const result: string[] = [];
    let currentField = '';
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      
      if (char === '"') {
        if (inQuotes && line[i + 1] === '"') {
          currentField += '"';
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (char === ',' && !inQuotes) {
        result.push(currentField.trim());
        currentField = '';
      } else {
        currentField += char;
      }
    }
    
    result.push(currentField.trim());
    return result;
  }

  private transform(raw: any, integration: Integration): any | null {
    const result: any = {
      platform_id: integration.platform_id
    };
    
    for (const [csvField, fieldMap] of Object.entries(integration.field_mapping)) {
      const value = raw[csvField];
      const target = fieldMap.target;
      
      // Apply transformations if specified
      let transformedValue = this.applyTransform(value, fieldMap);
      
      // Proper type checking with FieldMap
      if (fieldMap.required && (transformedValue === undefined || transformedValue === null || transformedValue === '')) {
        return null;
      }
      
      result[target] = transformedValue;
    }

    // Special handling for DeliveryPlatform2 date/time combination
    if (integration.name === 'deliveryplatform2_business_segments' && result.order_datetime && result.order_time) {
      const dateValue = result.order_datetime instanceof Date ? result.order_datetime.toISOString().split('T')[0] : result.order_datetime;
      const timeValue = result.order_time;
      result.order_datetime = DataTransforms.parseDeliveryPlatform2DateTime(dateValue, timeValue);
      delete result.order_time; // Remove the temporary time field
    }

    // Determine order_status for orders
    if (integration.tables.includes('orders')) {
      result.order_status = this.determineOrderStatus(result, integration);
      
      if (!result.platform_order_id || result.platform_order_id === '') {
        return null;
      }
    }

    return result;
  }

  private applyTransform(value: string, fieldMap: FieldMap): any {
    if (!value && fieldMap.default !== undefined) {
      return fieldMap.default;
    }

    // Apply custom transformations first
    if (fieldMap.transform) {
      return this.runTransformation(value, fieldMap.transform);
    }

    switch (fieldMap.type) {
      case 'number':
        if (!value || value === '') return null;
        const num = parseFloat(String(value).replace(/[^0-9.-]/g, ''));
        return isNaN(num) ? null : num;
      
      case 'boolean':
        if (!value || value === '') return false;
        const strValue = String(value).toLowerCase();
        return strValue === 'on' || strValue === 'true' || strValue === '1';
      
      case 'date':
        if (!value || value === '') return null;
        const date = new Date(String(value));
        return isNaN(date.getTime()) ? null : date;
      
      case 'enum':
        return this.transformEnum(value, fieldMap);
      
      default:
        return value ? String(value).trim() || null : null;
    }
  }

  private runTransformation(value: string, transformName: string): any {
    return DataTransforms.runTransformation(value, transformName);
  }

  private transformEnum(value: string, fieldMap: FieldMap): any {
    if (!fieldMap.enum_values) return value;
    
    const normalizedValue = value?.toLowerCase();
    const match = fieldMap.enum_values.find(enumVal => 
      enumVal.toLowerCase() === normalizedValue
    );
    
    return match || value;
  }

  private determineOrderStatus(result: any, integration: Integration): OrderStatus {
    const integrationName = integration.name;
    
    if (integrationName === 'deliveryplatform1_order_history') {
      // Already transformed by deliveryPlatform1OrderStatus
      if (result.order_status) return result.order_status;
      
      // Fallback logic
      if (result.cancelled_by) {
        return result.cancelled_by === 'customer' ? 'CANCELLED_CUSTOMER' : 'CANCELLED_RESTAURANT';
      }
      return result.completed_flag ? 'COMPLETED' : 'REJECTED';
    }
    
    if (integrationName === 'deliveryplatform3_total_order') {
      const customerCancelled = parseInt(result.customer_cancelled_count || '0');
      const partnerCancelled = parseInt(result.partner_cancelled_count || '0');
      
      if (customerCancelled > 0) return 'CANCELLED_CUSTOMER';
      if (partnerCancelled > 0) return 'CANCELLED_RESTAURANT';
      
      // Check order status
      const status = result.order_status_raw?.toLowerCase();
      if (status === 'good') return 'COMPLETED';
      if (status === 'bad') return 'REJECTED';
      
      return 'ACCEPTED'; // Default for DeliveryPlatform3
    }
    
    if (integrationName === 'deliveryplatform2_business_segments') {
      // For DeliveryPlatform2, all orders in the data are accepted/completed IG?
      // Could check if prep_time_minutes > 0 to determine acceptance
      return result.prep_time_minutes && result.prep_time_minutes > 0 ? 'ACCEPTED' : 'COMPLETED';
    }
    
    return 'ACCEPTED'; // Default
  }

  private async processRecord(record: any, integration: Integration): Promise<void> {
    try {
      const tables = integration.tables || [];
      
      // Always upsert restaurant if we have restaurant data
      let restaurantId: number = 0;
      if (record.restaurant_name && typeof record.restaurant_name === 'string') {
        const restaurantName: string = record.restaurant_name;
        const restaurantExternalId: string | undefined = 
          (record.restaurant_external_id && typeof record.restaurant_external_id === 'string') 
            ? record.restaurant_external_id 
            : undefined;
            
        restaurantId = await this.db.upsertRestaurant(
          restaurantName,
          integration.platform_id,
          restaurantExternalId
        );
      }

      // Create orders if this integration targets orders table
      if (tables.includes('orders')) {
        const orderData: OrderData = {
          platform_id: integration.platform_id,
          platform_order_id: String(record.platform_order_id),
          restaurant_id: restaurantId,
          order_status: record.order_status || 'ACCEPTED',
          delivery_type: record.delivery_type || 'UNKNOWN',
          order_value: typeof record.order_value === 'number' ? record.order_value : null,
          basket_size: typeof record.basket_size === 'number' ? record.basket_size : null,
          discount_amount: typeof record.discount_amount === 'number' ? record.discount_amount : null,
          order_datetime: record.order_datetime instanceof Date ? record.order_datetime : null,
          restaurant_wait_time_minutes: typeof record.restaurant_wait_time_minutes === 'number' ? record.restaurant_wait_time_minutes : null,
          total_delivery_time_minutes: typeof record.total_delivery_time_minutes === 'number' ? record.total_delivery_time_minutes : null,
          courier_wait_time_minutes: typeof record.courier_wait_time_minutes === 'number' ? record.courier_wait_time_minutes : null,
          prep_time_minutes: typeof record.prep_time_minutes === 'number' ? record.prep_time_minutes : null,
          currency_code: record.currency_code || 'GBP',
          auto_accept_status: record.auto_accept_status || null
        };
        
        await this.db.upsertOrder(orderData);
      }

      // Create ratings if this integration targets ratings table
      if (tables.includes('ratings') && 
          record.rating_value !== null && 
          record.rating_value !== undefined && 
          typeof record.rating_value === 'number' && 
          !isNaN(record.rating_value) && 
          restaurantId > 0) {
            
        const ratingData = {
          platform_order_id: String(record.platform_order_id),
          rating_value: record.rating_value,
          rating_type: 'overall',
          comment: record.comment || null
        };
        
        await this.db.upsertRating(ratingData, restaurantId, integration.platform_id);
      }

    } catch (error: unknown) {
      if (error instanceof ProcessingError) {
        throw error;
      }
      throw new ProcessingError('Failed to process record', { 
        originalError: error, 
        record, 
        integration: integration.name 
      });
    }
  }

  private async hash(path: string): Promise<string> {
    return new Promise((resolve, reject) => {
      const hash = crypto.createHash('sha256');
      const stream = fs.createReadStream(path);
      stream.on('data', data => hash.update(data));
      stream.on('end', () => resolve(hash.digest('hex')));
      stream.on('error', reject);
    });
  }
} 