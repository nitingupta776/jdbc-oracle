import java.sql.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.UUID;  

public class JDBCStoreImage {
	public static void main(String args[]) {
		try {
			try {
				Class.forName("oracle.jdbc.OracleDriver");
			} catch (Exception ex) { }

			Connection con = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521/orcl", "system", "oracle");
			FileInputStream fileInputStream = null; 

			PreparedStatement preparedStatement = con.prepareStatement("INSERT INTO staff (name, timestamp, uuid, image) VALUES (?,?,?,?)");

			preparedStatement.setString(1, "BBB");

			preparedStatement.setTimestamp(2, new Timestamp(System.currentTimeMillis()));

			UUID uuid=UUID.randomUUID();

			//Generates random UUID 
			preparedStatement.setBytes(3,asBytes(uuid));

			File file= new File("/Users/bhavukgupta/Desktop/Lab2/car.jpg");
			try {
				fileInputStream = new FileInputStream(file);
				preparedStatement.setBinaryStream(4, fileInputStream,fileInputStream.available());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			con.commit();
			con.setAutoCommit(true);

			int numberOfRowsInserted = preparedStatement.executeUpdate();

			System.out.println("numberOfRowsInserted : " + numberOfRowsInserted + "\n");

			PreparedStatement stmt2 = con.prepareStatement("SELECT * FROM staff");
			ResultSet rs = stmt2.executeQuery();

			FileOutputStream fileOutputStream = null;
			
			System.out.println("NAME" + "\t" + "TIMEPSTAMP" + "\t\t\t" + "UUID" +  "\t\t\t\t\t" + "IMAGE STATUS"); 		

			int imageNumber = 0;
			//Retrieve the image
			while (rs.next()) {
				String sname = rs.getString(1);
				String stimestamp = rs.getString(2);
				String suuid = rs.getString(3);

				Blob clob = rs.getBlob(4);
				byte[] byteArr = clob.getBytes(1,(int)clob.length());

				fileOutputStream = new FileOutputStream("/Users/bhavukgupta/Desktop/Lab2/test" + imageNumber + ".jpg");

				fileOutputStream.write(byteArr);  
				imageNumber++;
				
				System.out.println(sname + "\t" + stimestamp + "\t\t" + suuid +  "\t" + "Image " + imageNumber+ " retrieved successfully"); 		
				//close connection
				fileOutputStream.close();
			}
			


			preparedStatement.close();
			con.close();

		}
		catch(SQLException ex) { 
			System.out.println("\n--- SQLException caught ---\n"); 
			while (ex != null) { 
				System.out.println("Message: " + ex.getMessage ()); 
				System.out.println("SQLState: " + ex.getSQLState ()); 
				System.out.println("ErrorCode: " + ex.getErrorCode ()); 
				ex = ex.getNextException(); 
				System.out.println("");
			} 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	} 

	public static byte[] asBytes(UUID uuid) {
		ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
		bb.putLong(uuid.getMostSignificantBits());
		bb.putLong(uuid.getLeastSignificantBits());
		return bb.array();
	}
}