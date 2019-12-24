package hdfs;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//맵
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
	
	
	private Text dustLevelAndWindAngle = new Text();//미세먼지 농도 및 바람의 방향
    private IntWritable countOne = new IntWritable(1);
    
    //한 줄씩 입력받음
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	//공백을 기준으로 문자열을 분할 - 날짜, 풍향, 미세먼지, 초미세먼지, 강우량
    	System.out.println("들어온 줄 내용: " + value);
    	StringTokenizer itr = new StringTokenizer(value.toString());
        
        System.out.println("날짜: " + itr.nextToken());//첫 번째 요소는 날짜, 스킵
    		
        int windAngle = Integer.parseInt(itr.nextToken());//풍향
        String windAngleString;
        if((0 <= windAngle && windAngle <= 45) || (315 < windAngle && windAngle <= 360)) {//북풍
        	windAngleString = WIND_NORTH;
        } else if(45 < windAngle && windAngle <= 120) {//동풍
        	windAngleString = WIND_EAST;
        } else if(120 < windAngle && windAngle < 225) {//남풍
        	windAngleString = WIND_SOUTH;
        } else
        	windAngleString = WIND_WEST;
        
        System.out.println("풍향: " + windAngleString);
        float dust = Float.parseFloat(itr.nextToken());//미세먼지
        System.out.println("미세먼지: " + dust);
        float microDust = Float.parseFloat(itr.nextToken());//초미세먼지
        System.out.println("초미세먼지: " + microDust);
        
        if(!itr.hasMoreTokens()) {//비가 오지 않았다면
    	 	if(dust > DUST_NOMAL || microDust > MICRODUST_NOMAL) {//미세먼지 나쁨
    	 		dustLevelAndWindAngle.set(DUST_LEVEL_SERIOUS + ": " + windAngleString);
    			System.out.println("미세먼지 나쁨");
    		}
    		else if(dust > DUST_CLEAN || microDust > MICRODUST_CLEAN) {//미세먼지가 보통
    			dustLevelAndWindAngle.set(DUST_LEVEL_NOMAL + ": " + windAngleString);
    			System.out.println("미세먼지 보통");
    		}
    		else {//미세먼지가 좋음
    			dustLevelAndWindAngle.set(DUST_LEVEL_CLEAN + ": " + windAngleString);
    			System.out.println("미세먼지 좋음");
    		}
    		
    		// context 객체를 이용하여 임시파일로 저장
    		context.write(dustLevelAndWindAngle, countOne);//미세먼지 수준과 풍향을 같이 날림
        }
    }
}