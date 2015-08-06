package hadooprun;

import java.net.UnknownHostException;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;
 
/**
 * <b>function:</b>MongoDB ��ʾ��
 * @author hoojo
 * @createDate 2011-5-24 ����02:42:29
 * @file SimpleTest.java
 * @package com.hoo.test
 * @project MongoDB
 * @blog http://blog.csdn.net/IBM_hoojo
 * @email hoojo_@126.com
 * @version 1.0
 */
public class test2 {
 
    public static void main(String[] args) throws UnknownHostException, MongoException {
        Mongo mg = new Mongo("192.168.142.132:27017");
        //��ѯ���е�Database
        for (String name : mg.getDatabaseNames()) {
            System.out.println("dbName: " + name);
        }
        
        DB db = mg.getDB("hadoop");
        //��ѯ���еľۼ�����
        for (String name : db.getCollectionNames()) {
            System.out.println("collectionName: " + name);
        }
        
        DBCollection users = db.getCollection("test");
        
        //��ѯ���е�����
        DBCursor cur = users.find();
        while (cur.hasNext()) {
            System.out.println(cur.next());
        }
        System.out.println(cur.count());
        System.out.println(cur.getCursorId());
        System.out.println(JSON.serialize(cur));
    }
}