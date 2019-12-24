package hdfs;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//��
public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static float DUST_CLEAN = 30;
	private final static float DUST_NOMAL = 80;
	private final static float MICRODUST_CLEAN = 15;
	private final static float MICRODUST_NOMAL = 35;
	public final static String DUST_LEVEL_CLEAN = "clean";
	public final static String DUST_LEVEL_NOMAL = "nomal";
	public final static String DUST_LEVEL_SERIOUS = "serious";
	public final static String WIND_NORTH = "NORTH";
	public final static String WIND_SOUTH = "SOUTH";
	public final static String WIND_EAST = "EAST";
	public final static String WIND_WEST = "WEST";
	
	
	private Text dustLevelAndWindAngle = new Text();//�̼����� �� �� �ٶ��� ����
    private IntWritable countOne = new IntWritable(1);
    
    //�� �پ� �Է¹���
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	//������ �������� ���ڿ��� ���� - ��¥, ǳ��, �̼�����, �ʹ̼�����, ���췮
    	System.out.println("���� �� ����: " + value);
    	StringTokenizer itr = new StringTokenizer(value.toString());
        
        System.out.println("��¥: " + itr.nextToken());//ù ��° ��Ҵ� ��¥, ��ŵ
    		
        int windAngle = Integer.parseInt(itr.nextToken());//ǳ��
        String windAngleString;
        if((0 <= windAngle && windAngle <= 45) || (315 < windAngle && windAngle <= 360)) {//��ǳ
        	windAngleString = WIND_NORTH;
        } else if(45 < windAngle && windAngle <= 120) {//��ǳ
        	windAngleString = WIND_EAST;
        } else if(120 < windAngle && windAngle < 225) {//��ǳ
        	windAngleString = WIND_SOUTH;
        } else
        	windAngleString = WIND_WEST;
        
        System.out.println("ǳ��: " + windAngleString);
        float dust = Float.parseFloat(itr.nextToken());//�̼�����
        System.out.println("�̼�����: " + dust);
        float microDust = Float.parseFloat(itr.nextToken());//�ʹ̼�����
        System.out.println("�ʹ̼�����: " + microDust);
        
        if(!itr.hasMoreTokens()) {//�� ���� �ʾҴٸ�
    	 	if(dust > DUST_NOMAL || microDust > MICRODUST_NOMAL) {//�̼����� ����
    	 		dustLevelAndWindAngle.set(DUST_LEVEL_SERIOUS + ": " + windAngleString);
    			System.out.println("�̼����� ����");
    		}
    		else if(dust > DUST_CLEAN || microDust > MICRODUST_CLEAN) {//�̼������� ����
    			dustLevelAndWindAngle.set(DUST_LEVEL_NOMAL + ": " + windAngleString);
    			System.out.println("�̼����� ����");
    		}
    		else {//�̼������� ����
    			dustLevelAndWindAngle.set(DUST_LEVEL_CLEAN + ": " + windAngleString);
    			System.out.println("�̼����� ����");
    		}
    		
    		// context ��ü�� �̿��Ͽ� �ӽ����Ϸ� ����
    		context.write(dustLevelAndWindAngle, countOne);//�̼����� ���ذ� ǳ���� ���� ����
        }
    }
}