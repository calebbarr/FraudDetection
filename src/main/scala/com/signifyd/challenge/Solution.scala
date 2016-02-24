package com.xbarr.frauddetection

import com.github.tototoshi.csv.CSVReader
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import java.io.File

object Solution {
  
  val dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
  
  def main(args:Array[String]):Unit = 
    getStatuses(ingest(args(0))) foreach println
  
  def ingest(filename:String) =
    CSVReader.open(new File(filename)).iterator.map { r =>  
      EventRecord(dateFormatter.parseDateTime(r(0)),r(1),r(2))}
  
  def getStatuses(records:Iterator[EventRecord]) = {
      val first = records.next
      records.foldLeft((Map(first.email->Account.initialize(first)),List(first.status(Account.status)))){ 
        case((accounts,statuses),record) => {
          val account = 
            if(accounts.contains(record.email)) accounts(record.email).update(record) else Account.initialize(record)
          val status = record.status(
              if(accounts.contains(record.email)) accounts(record.email).update(record.date).status else Account.status)
          (accounts ++ Map( account.email -> account),statuses.+:(status))
        } }._2.filterNot(_.isEmpty).reverse.iterator // reverse because values are prepended to immutable list   
  }
  
  case class EventRecord(date:DateTime, email:String, eventType:String){
    def isFraud = this.eventType == "FRAUD_REPORT"
    def isPurchase = this.eventType == "PURCHASE"
    def isOld(today:DateTime) = this.date.isOld(today)
    def status(accountStatus:String) =
      if(this.isPurchase) 
        List(
            this.date.toString("yyyy-MM-dd"),
            this.email,
            accountStatus
        ).mkString(",")
      else ""
  }
  
  case class Account(email:String, fraudCount:Int, goodHistory:Int,records:List[EventRecord]) {
    def update(record:EventRecord) = // could throw exception if names aren't the same
      Account(
          this.email,
          fraudCount = this.fraudCount + {if(record.isFraud) 1 else 0},
          goodHistory = this.goodHistory + this.records.filter{_.isOld(record.date)}.size,
          records = records.+:(record).filterNot{_.isOld(record.date)}
      )
      
   def update(today:DateTime) =
     Account(
         email = this.email,
         fraudCount = this.fraudCount,
         goodHistory = this.goodHistory + this.records.filter{_.isOld(today)}.size,
         records = this.records.filterNot{_.isOld(today)}
     )
      
   def status =
     if(this.fraudCount > 0) s"FRAUD_HISTORY:${this.fraudCount}"
     else if(this.goodHistory > 0) s"GOOD_HISTORY:${this.goodHistory}"
     else s"UNCONFIRMED_HISTORY:${this.records.size}"
  }
  
  case object Account {
    def initialize(record:EventRecord) = 
      Account(
        record.email,
        fraudCount = if(record.isFraud) 1 else 0,
        goodHistory = 0,
        records = if(record.isFraud) List() else List(record)
      )
    def status = "NO_HISTORY"
  }
  
  implicit class EnhancedDateTime(d:DateTime) {
    
    def isOld(today:DateTime) = today.daysAheadOf(d) > 90
    
    def daysAheadOf(d2:DateTime) =
      ((d.getMillis() - d2.getMillis()) / (1000 * 60 * 60 * 24))
    
  }
  
}