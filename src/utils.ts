import * as fs from 'fs';

// Standardized error handling
export interface AppError extends Error {
  code: string;
  context?: Record<string, any>;
}

export class ValidationError extends Error implements AppError {
  code = 'VALIDATION_ERROR';
  context: Record<string, any>;

  constructor(message: string, context?: Record<string, any>) {
    super(message);
    this.name = 'ValidationError';
    this.context = context || {};
  }
}

export class ProcessingError extends Error implements AppError {
  code = 'PROCESSING_ERROR';
  context: Record<string, any>;

  constructor(message: string, context?: Record<string, any>) {
    super(message);
    this.name = 'ProcessingError';
    this.context = context || {};
  }
}

export class DatabaseError extends Error implements AppError {
  code = 'DATABASE_ERROR';
  context: Record<string, any>;

  constructor(message: string, context?: Record<string, any>) {
    super(message);
    this.name = 'DatabaseError';
    this.context = context || {};
  }
}

// Input validation utilities
export class Validators {
  static isValidFilePath(path: string): boolean {
    return Boolean(path && path.length > 0 && fs.existsSync(path));
  }

  static isValidIntegrationKey(key: string): boolean {
    return /^[a-z_]+$/.test(key);
  }

  static isValidCommand(command: string): boolean {
    return ['process', 'analyse'].includes(command);
  }

  static isValidString(value: any): boolean {
    return typeof value === 'string' && value.trim().length > 0;
  }

  static isValidNumber(value: any): boolean {
    return typeof value === 'number' && !isNaN(value);
  }

  static isValidDate(value: any): boolean {
    return value instanceof Date && !isNaN(value.getTime());
  }
}

// Standardized logging
export class Logger {
  static info(message: string, context?: Record<string, any>) {
    console.log(`ℹ️  ${message}`, context ? JSON.stringify(context) : '');
  }

  static error(message: string, error?: Error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error(`❌ ${message}: ${errorMessage}`);
  }

  static success(message: string) {
    console.log(`✅ ${message}`);
  }

  static warn(message: string) {
    console.log(`⚠️  ${message}`);
  }
}

// Error handling utility
export function handleError(error: unknown, context: string): never {
  if (error instanceof ValidationError || 
      error instanceof ProcessingError || 
      error instanceof DatabaseError) {
    Logger.error(`${context} failed`, error);
  } else {
    Logger.error(`Unexpected error during ${context}`, error as Error);
  }
  process.exit(1);
} 