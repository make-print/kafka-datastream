import dotenv from 'dotenv';
import fs from "fs";

dotenv.config();


export const confluent = {
  cloudConfig: {
    'bootstrap.servers': process.env.BOOTSTRAP_SERVERS || '',
    'sasl.username': process.env.SASL_USERNAME || '',
    'sasl.password': process.env.SASL_PASSWORD || '',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
  },
  topic: process.env.TOPIC || '',
};


export const readConfig = (fileName: string) => {
  const data = fs.readFileSync(fileName, "utf8").toString().split("\n");
  return data.reduce((config: any, line: string) => {
    const [key, value] = line.split("=");
    if (key && value) {
      config[key] = value;
    }
    return config;
  }, {});
}


