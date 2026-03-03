# Spark-Streaming-HDFS-Monitoring-System
I developed a real-time Big Data streaming application using Apache Spark Streaming and Scala, deployed on an EMR-based Hadoop ecosystem. This project involved building a fully functional Spark Streaming pipeline capable of monitoring HDFS in real time, processing streaming text data every 3 seconds, and generating structured analytical outputs.


#  Real-Time Word & Bigram Frequency Engine  
**Spark Structured Streaming | Scala | Parquet | YARN-Ready**

---

##  Project Overview

This project implements a **real-time text analytics pipeline** using **Apache Spark Structured Streaming (Scala)**.

The system ingests streaming text data, performs text cleansing and tokenization, and computes:

-  Word frequency counts  
-  Adjacent word-pair (bigram) frequency counts  

The results are written to **Parquet storage** with **checkpointing support**, making the pipeline fault-tolerant and production-ready.

---

## 🏗 Architecture

Streaming Text Files  
↓  
Spark Structured Streaming  
↓  
Text Cleaning & Normalization  
↓  
Tokenization  
↓  
Word Count + Bigram Aggregation  
↓  
Parquet Sink (Checkpointed)  

---

## ⚙️ Key Features

 Real-time ingestion from streaming file directory  
 Text normalization (lowercasing + regex cleaning)  
 Tokenization using Spark SQL functions  
 Word frequency aggregation  
 Bigram (word-pair) generation  
 Checkpointing for fault tolerance  
 Parquet output sink  
 YARN cluster compatible  
 Configurable via command-line arguments  

---

