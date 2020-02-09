package org.sda.universityjdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;

public class App {
	private static final String DB_URL = "jdbc:mysql://localhost:3306/university_sda10";
	private static final String USERNAME = "root";
	private static final String PASSWORD = "admin";

	private static Connection connection;

	public static void main(String[] args) throws SQLException {
		connection = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
		
		getCourses();
		
		System.out.println("--------------------------------------------");

		getProfessors();
		
		insertDepartment();
		
		updateCourse();
		
		updateCourse("Optional", "Fizica aplicata");
		
		printCourseByName("Fizica aplicata");
		
		saveStudentInTransaction("Test transaction success", 1, 1);
		saveStudentInTransaction("Test transaction failure", 1, 150);

		connection.close();
	}
	
	private static void getCourses() throws SQLException {
		String sql = "select * from curs";

		Statement statement = connection.createStatement();

		ResultSet rs = statement.executeQuery(sql);

		while (rs.next()) {
			int id = rs.getInt("id_curs");
			String name = rs.getString("nume");
			int credits = rs.getInt("credite");
			String description = rs.getString("descriere");
			int department_id = rs.getInt("id_departament");

			System.out.println(id + " " + name + " " + credits + " " + description + " " + department_id);
		}

		rs.close();
		statement.close();
	}
	
	private static void getProfessors() throws SQLException {
		String profesorSql = "select p.id_profesor, p.nume, p.adresa, p.salariu, "
				+ "p.data_angajare, d.nume from profesor p join departament d on "
				+ "p.id_departament = d.id_departament";

		Statement statement2 = connection.createStatement();

		ResultSet resultSet = statement2.executeQuery(profesorSql);

		while (resultSet.next()) {
			String departmentName = resultSet.getString("d.nume");
			int profId = resultSet.getInt("p.id_profesor");
			String profName = resultSet.getString("p.nume");
			String address = resultSet.getString("p.adresa");
			int salary = resultSet.getInt("p.salariu");
			LocalDate hireDate = resultSet.getDate("p.data_angajare").toLocalDate();

			System.out.println(
					profId + " " + profName + " " + address + " " + salary + " " + 
			hireDate + " " + departmentName);
		}
		
		resultSet.close();
		statement2.close();
	}
	
	private static void insertDepartment() throws SQLException {
		String sql = "insert into departament(nume) values('Java')";
		
		Statement statement = connection.createStatement();
		
		statement.executeUpdate(sql);
		
		statement.close();
	}
	
	private static void updateCourse() throws SQLException {
		String sql = "update curs set curs.descriere='Obligatoriu' where curs.nume='SQL'";
		
		Statement statement = connection.createStatement();
		
		statement.executeUpdate(sql);
		
		statement.close();
	}
	
	private static void updateCourse(String description, String courseName) throws SQLException {
		String sql = "update curs set curs.descriere=? where curs.nume=?";
		
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setString(1, description);
		preparedStatement.setString(2, courseName);
		
		preparedStatement.executeUpdate();
		
		preparedStatement.close();
	}
	
	private static void printCourseByName(String courseName) throws SQLException {
		String sql = "select * from curs where curs.nume=?";
		
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setString(1, courseName);
		
		ResultSet rs = preparedStatement.executeQuery();
		
		while(rs.next()) {
			int id = rs.getInt("id_curs");
			String name = rs.getString("nume");
			int credits = rs.getInt("credite");
			String description = rs.getString("descriere");
			int department_id = rs.getInt("id_departament");
			
			System.out.println(id + " " + name + " " + credits + " " + description + " " + department_id);
		}
		
		rs.close();
		preparedStatement.close();
	}
	
	private static void saveStudentInTransaction(String name, int profId, int sectionId) {
		String studentSql = "insert into student(nume, id_profesor) values(?, ?)";
		
		try {
			connection.setAutoCommit(false);
			
			PreparedStatement studentStatement = connection.prepareStatement(studentSql, Statement.RETURN_GENERATED_KEYS);
			studentStatement.setString(1, name);
			studentStatement.setInt(2, profId);
			
			studentStatement.executeUpdate();
			
			ResultSet rs = studentStatement.getGeneratedKeys();
			rs.next();
			int studentId = rs.getInt(1);
			
			rs.close();
			studentStatement.close();
			
			String sectionSql = "insert into sectie_student(id_sectie, id_student) values(?,?)";
			PreparedStatement sectionStatement = connection.prepareStatement(sectionSql);
			sectionStatement.setInt(1, sectionId);
			sectionStatement.setInt(2, studentId);
			
			sectionStatement.executeUpdate();
			
			sectionStatement.close();
			
			connection.commit();
			connection.setAutoCommit(true);
		} catch (SQLException e) {
			try {
				connection.rollback();
				connection.setAutoCommit(true);
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		}
		
	}
}
