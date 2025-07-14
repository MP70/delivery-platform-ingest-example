
export interface Platform {
  id: number;
  name: string;
  created_at: Date;
}

export interface Restaurant {
  id: number;
  name: string;
  platform_id: number;
  external_id?: string;
  created_at: Date;
}

export type OrderStatus = 
  | 'ACCEPTED'
  | 'REJECTED'
  | 'REJECTED_CUSTOMER'
  | 'REJECTED_RESTAURANT'
  | 'CANCELLED_CUSTOMER'
  | 'CANCELLED_RESTAURANT'
  | 'COMPLETED';

export type DeliveryType = 
  | 'DELIVERY'
  | 'COLLECTION'
  | 'PICKUP'
  | 'UNKNOWN';

export type TransformFunction = (value: string) => any;

export interface OrderData {
  id?: number;
  platform_id: number;
  platform_order_id: string;
  restaurant_id: number;
  order_status: OrderStatus;
  delivery_type?: DeliveryType;
  order_value?: number;
  basket_size?: number;
  discount_amount?: number;
  order_datetime?: Date;
  restaurant_wait_time_minutes?: number;
  total_delivery_time_minutes?: number;
  courier_wait_time_minutes?: number;
  prep_time_minutes?: number;
  currency_code?: string;
  auto_accept_status?: string;
  created_at?: Date;
}

export interface Rating {
  id?: number;
  restaurant_id: number;
  platform_id: number;
  platform_order_id?: string;
  rating_value: number;
  rating_type: string;
  comment?: string;
  rating_date?: Date;
  created_at?: Date;
}

export interface FieldMap {
  target: string;
  type?: 'string' | 'number' | 'boolean' | 'date' | 'enum';
  enum_values?: string[];
  transform?: string;
  required?: boolean;
  default?: any;
}

export interface Integration {
  id?: number;
  name: string;
  platform_id: number;
  field_mapping: Record<string, FieldMap>;
  tables: string[];
  is_active: boolean;
  created_at?: Date;
}

export type JobStatus = 'pending' | 'completed' | 'failed';

export interface IngestionJob {
  id?: number;
  integration_id: number;
  file_path: string;
  status: JobStatus;
  total_rows: number;
  processed_rows: number;
  inserted_rows: number;
  error_rows: number;
  error_message?: string;
  started_at?: Date;
  completed_at?: Date;
}

export interface DataSourceFile {
  id?: number;
  integration_id: number;
  file_path: string;
  file_hash: string;
  total_rows: number;
  job_id?: number;
  processed_at?: Date;
}

export interface JobUpdate {
  id?: number;
  integration_id?: number;
  file_path?: string;
  status?: JobStatus;
  total_rows?: number;
  processed_rows?: number;
  inserted_rows?: number;
  error_rows?: number;
  error_message?: string;
  started_at?: Date;
  completed_at?: Date;
} 