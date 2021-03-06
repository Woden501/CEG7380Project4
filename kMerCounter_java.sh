hadoop fs -mkdir colbySnedekerProject4/input
hadoop fs -put ecoli.fa colbySnedekerProject4/input

mkdir kMerCounter_class
javac -classpath /opt/hadoop/hadoop-core-1.2.1.jar -d kMerCounter_class KMerCounter.java
jar -cvf KMerCounter.jar -C /home/vcslstudent/colbySnedekerProject4/kMerCounter_class/ .
hadoop jar /home/vcslstudent/colbySnedekerProject4/KMerCounter.jar snedeker.cc.project4.KMerCounter 10 /user/vcslstudent/colbySnedekerProject4/input/ecoli.fa /user/vcslstudent/colbySnedekerProject4/KMerCounter_out10_java
hadoop jar /home/vcslstudent/colbySnedekerProject4/KMerCounter.jar snedeker.cc.project4.KMerCounter 20 /user/vcslstudent/colbySnedekerProject4/input/ecoli.fa /user/vcslstudent/colbySnedekerProject4/KMerCounter_out20_java

hadoop fs -cat /user/vcslstudent/colbySnedekerProject4/KMerCounter_out10_java/part-r-00000
hadoop fs -cat /user/vcslstudent/colbySnedekerProject4/KMerCounter_out20_java/part-r-00000