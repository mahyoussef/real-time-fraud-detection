package com.flinklearn.realtime.pojo;

public class Transaction {
    public String TransactionId = "";
    public String Timestamp = "";
    public String City = "";
    public String CustomerId = "";
    public String AccountLink = "";
    public String CreditCardNumber = "";
    public float TransactionAmount = 0;

    public Transaction(){}

    public Transaction(String transaction)
    {
        String[] words = transaction.split(",");
        this.TransactionId = words[0];
        this.Timestamp = words[1];
        this.City = words[2];
        this.CustomerId = words[3];
        this.AccountLink = words[4];
        this.CreditCardNumber = words[5];
        this.TransactionAmount = Float.parseFloat(words[6]);
    }
}
