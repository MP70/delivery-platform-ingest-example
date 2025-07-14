import { OrderStatus, DeliveryType } from './types.js';
import { ProcessingError, Validators } from './utils.js';

export class DataTransforms {
  // Utility functions
  static existsOrEmpty(value: string): string {
    return value || '';
  }

  static existsOrDefault(value: string, defaultValue: string = 'unknown'): string {
    return value || defaultValue;
  }

  static parseBoolean(value: string, trueValue: string = '1'): boolean {
    return value === trueValue;
  }

  static parsePercentage(value: string): string {
    const num = parseFloat(value);
    return isNaN(num) ? 'unknown' : `${Math.round(num * 100)}%`;
  }

  static timeToMinutes(value: string): number | null {
    try {
      if (!Validators.isValidString(value)) return null;
      
      // Handle formats like "5.5" (minutes) or "00:05:30" (HH:MM:SS)
      if (value.includes(':')) {
        const parts = value.split(':');
        if (parts.length < 2 || parts.length > 3) {
          throw new ProcessingError('Invalid time format', { value, expectedFormat: 'HH:MM or HH:MM:SS' });
        }
        
        const hours = parseInt(parts[0]) || 0;
        const minutes = parseInt(parts[1]) || 0;
        const seconds = parseInt(parts[2]) || 0;
        
        if (hours < 0 || minutes < 0 || minutes > 59 || seconds < 0 || seconds > 59) {
          throw new ProcessingError('Invalid time values', { value, hours, minutes, seconds });
        }
        
        return hours * 60 + minutes + Math.round(seconds / 60);
      }
      
      const num = parseFloat(value);
      if (isNaN(num) || num < 0) {
        throw new ProcessingError('Invalid numeric time value', { value });
      }
      
      return Math.round(num);
    } catch (error: unknown) {
      if (error instanceof ProcessingError) {
        throw error;
      }
      throw new ProcessingError('Failed to parse time to minutes', { originalError: error, value });
    }
  }

  static parseDate(value: string): Date | null {
    try {
      if (!Validators.isValidString(value)) return null;
      
      // Handle DD/MM/YYYY HH:MM:SS format (UK format)
      const ukDateMatch = value.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})\s+(\d{1,2}):(\d{2}):(\d{2})$/);
      if (ukDateMatch) {
        const [, day, month, year, hour, minute, second] = ukDateMatch;
        const dayNum = parseInt(day);
        const monthNum = parseInt(month);
        const yearNum = parseInt(year);
        const hourNum = parseInt(hour);
        const minuteNum = parseInt(minute);
        const secondNum = parseInt(second);
        
        // Validate date components
        if (dayNum < 1 || dayNum > 31 || monthNum < 1 || monthNum > 12 || 
            hourNum < 0 || hourNum > 23 || minuteNum < 0 || minuteNum > 59 || 
            secondNum < 0 || secondNum > 59) {
          throw new ProcessingError('Invalid date/time components', { 
            value, day: dayNum, month: monthNum, year: yearNum, 
            hour: hourNum, minute: minuteNum, second: secondNum 
          });
        }
        
        const date = new Date(yearNum, monthNum - 1, dayNum, hourNum, minuteNum, secondNum);
        if (isNaN(date.getTime())) {
          throw new ProcessingError('Invalid date', { value });
        }
        return date;
      }
      
      // Handle standard ISO format
      const date = new Date(value);
      if (isNaN(date.getTime())) {
        throw new ProcessingError('Invalid date format', { value });
      }
      return date;
    } catch (error: unknown) {
      if (error instanceof ProcessingError) {
        throw error;
      }
      throw new ProcessingError('Failed to parse date', { originalError: error, value });
    }
  }

  static parseDeliveryPlatform2Time(value: string): string {
    // Just return the time value as-is for now, will be combined with date later
    return value || '';
  }

  static parseDeliveryPlatform2DateTime(dateValue: string, timeValue: string): Date | null {
    if (!dateValue || !timeValue) return null;
    
    // Parse date (YYYY-MM-DD format)
    const dateMatch = dateValue.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
    if (!dateMatch) return null;
    
    // Parse time (HH:MM format)
    const timeMatch = timeValue.match(/^(\d{1,2}):(\d{2})$/);
    if (!timeMatch) return null;
    
    const [, year, month, day] = dateMatch;
    const [, hour, minute] = timeMatch;
    
    const date = new Date(parseInt(year), parseInt(month) - 1, parseInt(day), parseInt(hour), parseInt(minute), 0);
    return isNaN(date.getTime()) ? null : date;
  }

  static deliveryType(value: string): DeliveryType {
    switch (value?.toLowerCase()) {
      case 'delivery': return 'DELIVERY';
      case 'collection': return 'COLLECTION';
      case 'pickup': return 'PICKUP';
      default: return 'UNKNOWN';
    }
  }

  // Platform-specific transforms
  static deliveryPlatform1OrderStatus(value: string): OrderStatus {
    switch (value?.toLowerCase()) {
      case 'completed': return 'COMPLETED';
      case 'canceled': return 'REJECTED_CUSTOMER';
      default: return 'REJECTED';
    }
  }

  static deliveryPlatform1Boolean(value: string): boolean {
    return DataTransforms.parseBoolean(value, '1');
  }

  static deliveryPlatform1CancelledBy(value: string): string {
    return DataTransforms.existsOrEmpty(value);
  }

  static deliveryPlatform3OrderStatus(value: string): string {
    return DataTransforms.existsOrEmpty(value);
  }

  static deliveryPlatform2AcceptStatus(value: string): string {
    // Check if it's 'On' or 'on' for accepted status
    if (value?.toLowerCase() === 'on') {
      return 'accepted';
    }
    // If it's a number (acceptance rate), convert to percentage
    const num = parseFloat(value);
    if (!isNaN(num)) {
      return DataTransforms.parsePercentage(value);
    }
    return DataTransforms.existsOrDefault(value);
  }

  // Master transform dispatcher
  static runTransformation(value: string, transformName: string): any {
    switch (transformName) {
      // Utility transforms
      case 'existsOrEmpty':
        return DataTransforms.existsOrEmpty(value);
      case 'existsOrDefault':
        return DataTransforms.existsOrDefault(value);
      case 'parseBoolean':
        return DataTransforms.parseBoolean(value);
      case 'parsePercentage':
        return DataTransforms.parsePercentage(value);
      case 'timeToMinutes':
        return DataTransforms.timeToMinutes(value);
      case 'parseDate':
        return DataTransforms.parseDate(value);
      case 'parseDeliveryPlatform2Time':
        return DataTransforms.parseDeliveryPlatform2Time(value);
      case 'parseDeliveryPlatform2DateTime':
        // This requires both date and time values - will be handled specially
        return DataTransforms.parseDate(value);
      case 'deliveryType':
        return DataTransforms.deliveryType(value);
      
      // Platform-specific transforms
      case 'deliveryPlatform1OrderStatus':
        return DataTransforms.deliveryPlatform1OrderStatus(value);
      case 'deliveryPlatform1Boolean':
        return DataTransforms.deliveryPlatform1Boolean(value);
      case 'deliveryPlatform1CancelledBy':
        return DataTransforms.deliveryPlatform1CancelledBy(value);
      case 'deliveryPlatform3OrderStatus':
        return DataTransforms.deliveryPlatform3OrderStatus(value);
      case 'deliveryPlatform2AcceptStatus':
        return DataTransforms.deliveryPlatform2AcceptStatus(value);
      
      default:
        return value;
    }
  }
} 