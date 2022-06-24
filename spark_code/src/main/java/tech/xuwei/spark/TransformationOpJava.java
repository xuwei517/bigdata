package tech.xuwei.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 需求：Transformation实战
 * map：对集合中每个元素乘以2
 * filter：过滤出集合中的偶数
 * flatMap：将行拆分为单词
 * groupByKey：对每个大区的主播进行分组
 * reduceByKey：统计每个大区的主播数量
 * sortByKey：对主播的音浪收入排序
 * join：打印每个主播的大区信息和音浪收入
 * distinct：统计当天开播的大区信息
 * Created by xuwei
 */
public class TransformationOpJava {
    public static void main(String[] args) {
        JavaSparkContext sc = getSparkContext();
        //map：对集合中每个元素乘以2
        //mapOp(sc);
        //filter：过滤出集合中的偶数
        //filterOp(sc);
        //flatMap：将行拆分为单词
        //flatMapOp(sc);
        //groupByKey：对每个大区的主播进行分组
        //groupByKeyOp(sc);
        //groupByKeyOp2(sc);
        //reduceByKey：统计每个大区的主播数量
        //reduceByKeyOp(sc);
        //sortByKey：对主播的金币收入进行排序
        //sortByKeyOp(sc);
        //join：打印每个主播的大区信息和金币收入
        //joinOp(sc);
        //distinct：统计当天开播的大区信息
        //distinctOp(sc);

        sc.stop();
    }

    private static void distinctOp(JavaSparkContext sc) {
        Tuple2<Integer,String> t1 = new Tuple2<Integer,String>(150001, "US");
        Tuple2<Integer,String> t2 = new Tuple2<Integer,String>(150002, "CN");
        Tuple2<Integer,String> t3 = new Tuple2<Integer,String>(150003, "CN");
        Tuple2<Integer,String> t4 = new Tuple2<Integer,String>(150004, "IN");
        JavaRDD<Tuple2<Integer, String>> dataRDD = sc.parallelize(Arrays.asList(t1, t2, t3, t4));
        dataRDD.map(new Function<Tuple2<Integer, String>, String>() {
            @Override
            public String call(Tuple2<Integer, String> tup) throws Exception {
                return tup._2;
            }
        }).distinct().foreach(new VoidFunction<String>() {
            @Override
            public void call(String area) throws Exception {
                System.out.println(area);
            }
        });
    }

    private static void joinOp(JavaSparkContext sc) {
        Tuple2<Integer,String> t1 = new Tuple2<Integer,String>(150001, "US");
        Tuple2<Integer,String> t2 = new Tuple2<Integer,String>(150002, "CN");
        Tuple2<Integer,String> t3 = new Tuple2<Integer,String>(150003, "CN");
        Tuple2<Integer,String> t4 = new Tuple2<Integer,String>(150004, "IN");

        Tuple2<Integer,Integer> t5 = new Tuple2<Integer,Integer>(150001, 400);
        Tuple2<Integer,Integer> t6 = new Tuple2<Integer,Integer>(150002, 200);
        Tuple2<Integer,Integer> t7 = new Tuple2<Integer,Integer>(150003, 300);
        Tuple2<Integer,Integer> t8 = new Tuple2<Integer,Integer>(150004, 100);
        JavaRDD<Tuple2<Integer, String>> dataRDD1 = sc.parallelize(Arrays.asList(t1, t2, t3, t4));
        JavaRDD<Tuple2<Integer, Integer>> dataRDD2 = sc.parallelize(Arrays.asList(t5, t6, t7, t8));

        JavaPairRDD<Integer, String> dataRDD1Pair = dataRDD1.mapToPair(new PairFunction<Tuple2<Integer, String>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<Integer, String> tup)
                    throws Exception {
                return new Tuple2<Integer, String>(tup._1, tup._2);
            }
        });

        JavaPairRDD<Integer, Integer> dataRDD2Pair = dataRDD2.mapToPair(new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> tup)
                    throws Exception {
                return new Tuple2<Integer, Integer>(tup._1, tup._2);
            }
        });

        dataRDD1Pair.join(dataRDD2Pair).foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> tup) throws Exception {
                System.out.println(tup);
            }
        });
    }

    private static void sortByKeyOp(JavaSparkContext sc) {
        Tuple2<Integer,Integer> t1 = new Tuple2<Integer,Integer>(150001, 400);
        Tuple2<Integer,Integer> t2 = new Tuple2<Integer,Integer>(150002, 200);
        Tuple2<Integer,Integer> t3 = new Tuple2<Integer,Integer>(150003, 300);
        Tuple2<Integer,Integer> t4 = new Tuple2<Integer,Integer>(150004, 100);
        JavaRDD<Tuple2<Integer, Integer>> dataRDD = sc.parallelize(Arrays.asList(t1, t2, t3, t4));
        /*dataRDD.mapToPair(new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> tup)
                    throws Exception {
                return new Tuple2<Integer, Integer>(tup._2,tup._1);
            }
        }).sortByKey(false).foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void call(Tuple2<Integer, Integer> tup) throws Exception {
                System.out.println(tup);
            }
        });*/
        //使用sortBy
        dataRDD.sortBy(new Function<Tuple2<Integer, Integer>, Integer>() {
            @Override
            public Integer call(Tuple2<Integer, Integer> tup) throws Exception {
                return tup._2;
            }
        },false,1).foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void call(Tuple2<Integer, Integer> tup) throws Exception {
                System.out.println(tup);
            }
        });

    }


    private static void reduceByKeyOp(JavaSparkContext sc) {
        Tuple2<Integer,String> t1 = new Tuple2<Integer,String>(150001, "US");
        Tuple2<Integer,String> t2 = new Tuple2<Integer,String>(150002, "CN");
        Tuple2<Integer,String> t3 = new Tuple2<Integer,String>(150003, "CN");
        Tuple2<Integer,String> t4 = new Tuple2<Integer,String>(150004, "IN");
        JavaRDD<Tuple2<Integer,String>> dataRDD = sc.parallelize(Arrays.asList(t1, t2, t3, t4));
        dataRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tup)
                    throws Exception {
                return new Tuple2<String, Integer>(tup._2,1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tup) throws Exception {
                System.out.println(tup);
            }
        });

    }
    private static void groupByKeyOp(JavaSparkContext sc) {
        Tuple2<Integer,String> t1 = new Tuple2<Integer,String>(150001, "US");
        Tuple2<Integer,String> t2 = new Tuple2<Integer,String>(150002, "CN");
        Tuple2<Integer,String> t3 = new Tuple2<Integer,String>(150003, "CN");
        Tuple2<Integer,String> t4 = new Tuple2<Integer,String>(150004, "IN");
        JavaRDD<Tuple2<Integer,String>> dataRDD = sc.parallelize(Arrays.asList(t1, t2, t3, t4));

        //如果要使用XXXByKey之类的算子，则需要先使用XXXToPair算子
        dataRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tup)
                    throws Exception {
                return new Tuple2<String, Integer>(tup._2,tup._1);
            }
        }).groupByKey().foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> tup) throws Exception {
                //获取大区信息
                String area = tup._1;
                System.out.print(area+":");
                //获取同一个大区对应的所有用户ID
                Iterable<Integer> it = tup._2;
                for (Integer uid: it) {
                    System.out.print(uid+" ");
                }
                System.out.println();
            }
        });

    }

    private static void groupByKeyOp2(JavaSparkContext sc) {
        Tuple3<Integer,String,String> t1 = new Tuple3<Integer,String,String>(150001, "US", "male");
        Tuple3<Integer,String,String> t2 = new Tuple3<Integer,String,String>(150002, "CN", "female");
        Tuple3<Integer,String,String> t3 = new Tuple3<Integer,String,String>(150003, "CN", "male");
        Tuple3<Integer,String,String> t4 = new Tuple3<Integer,String,String>(150004, "IN", "female");
        JavaRDD<Tuple3<Integer,String,String>> dataRDD = sc.parallelize(Arrays.asList(t1, t2, t3, t4));

        dataRDD.mapToPair(new PairFunction<Tuple3<Integer, String, String>, String, Tuple2<Integer,String>>() {
            @Override
            public Tuple2<String, Tuple2<Integer, String>> call(Tuple3<Integer, String, String> tup) throws Exception {
                return new Tuple2<String, Tuple2<Integer, String>>(tup._2(), new Tuple2<Integer, String>(tup._1(), tup._3()));
            }
        }).groupByKey().foreach(new VoidFunction<Tuple2<String, Iterable<Tuple2<Integer, String>>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Tuple2<Integer, String>>> tup)
                    throws Exception {
                //获取大区信息
                String area = tup._1;
                System.out.print(area+":");
                //获取同一个大区对应的所有用户ID和性别信息
                Iterable<Tuple2<Integer, String>> it = tup._2;
                for (Tuple2<Integer, String> tu: it) {
                    System.out.print("<"+tu._1+","+tu._2+"> ");
                }
                System.out.println();
            }
        });

    }

    private static void flatMapOp(JavaSparkContext sc) {
        JavaRDD<String> dataRDD = sc.parallelize(Arrays.asList("good good study", "day day up"));
        dataRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                String[] words = line.split(" ");
                return Arrays.asList(words).iterator();
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String word) throws Exception {
                System.out.println(word);
            }
        });
    }

    private static void filterOp(JavaSparkContext sc) {
        JavaRDD<Integer> dataRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        dataRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer i1) throws Exception {
                return i1 % 2 == 0;
            }
        }).foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer i1) throws Exception {
                System.out.println(i1);
            }
        });
    }

    private static void mapOp(JavaSparkContext sc) {
        JavaRDD<Integer> dataRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        dataRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer i1) throws Exception {
                return i1 * 2;
            }
        }).foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer i1) throws Exception {
                System.out.println(i1);
            }
        });
    }

    private static JavaSparkContext getSparkContext() {
        SparkConf conf = new SparkConf();
        conf.setAppName("TransformationOpJava")
                .setMaster("local");
        return new JavaSparkContext(conf);
    }

}
