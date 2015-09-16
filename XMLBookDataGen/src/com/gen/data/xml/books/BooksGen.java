package com.gen.data.xml.books;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;

public class BooksGen {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try {
			String[] names = new String[] { "James", "John", "Robert",
					"Michael", "William", "David", "Richard", "Charles",
					"Joseph", "Thomas", "Christopher", "Daniel", "Paul",
					"Mark", "Donald", "George", "Kenneth", "Steven", "Edward",
					"Brian", "Ronald", "Anthony", "Kevin", "Jason", "Matthew",
					"Gary", "Timothy", "Jose", "Larry", "Jeffrey", "Frank",
					"Scott", "Eric", "Stephen", "Andrew", "Raymond", "Gregory",
					"Joshua", "Jerry", "Dennis", "Walter", "Patrick", "Peter",
					"Harold", "Douglas", "Henry", "Carl", "Arthur", "Ryan",
					"Roger", "Joe", "Juan", "Jack", "Albert", "Jonathan",
					"Justin", "Terry", "Gerald", "Keith", "Samuel", "Willie",
					"Ralph", "Lawrence", "Nicholas", "Roy", "Benjamin",
					"Bruce", "Brandon", "Adam", "Harry", "Fred", "Wayne",
					"Billy", "Steve", "Louis", "Jeremy", "Aaron", "Randy",
					"Howard", "Eugene", "Carlos", "Russell", "Bobby", "Victor",
					"Martin", "Ernest", "Phillip", "Todd", "Jesse", "Craig",
					"Alan", "Shawn", "Clarence", "Sean", "Philip", "Chris",
					"Johnny", "Earl", "Jimmy", "Antonio", "Danny", "Bryan",
					"Tony", "Luis", "Mike", "Stanley", "Leonard", "Nathan",
					"Dale", "Manuel", "Rodney", "Curtis", "Norman", "Allen",
					"Marvin", "Vincent", "Glenn", "Jeffery", "Travis", "Jeff",
					"Chad", "Jacob", "Lee", "Melvin", "Alfred", "Kyle",
					"Francis", "Bradley", "Jesus", "Herbert", "Frederick",
					"Ray", "Joel", "Edwin", "Don", "Eddie", "Ricky", "Troy",
					"Randall", "Barry", "Alexander", "Bernard", "Mario",
					"Leroy", "Francisco", "Marcus", "Micheal", "Theodore",
					"Clifford", "Miguel", "Oscar", "Jay", "Jim", "Tom",
					"Calvin", "Alex", "Jon", "Ronnie", "Bill", "Lloyd",
					"Tommy", "Leon", "Derek", "Warren", "Darrell", "Jerome",
					"Floyd", "Leo", "Alvin", "Tim", "Wesley", "Gordon", "Dean",
					"Greg", "Jorge", "Dustin", "Pedro", "Derrick", "Dan",
					"Lewis", "Zachary", "Corey", "Herman", "Maurice", "Vernon",
					"Roberto", "Clyde", "Glen", "Hector", "Shane", "Ricardo",
					"Sam", "Rick", "Lester", "Brent", "Ramon", "Charlie",
					"Tyler", "Gilbert", "Gene", "Marc", "Reginald", "Ruben",
					"Brett", "Angel", "Nathaniel", "Rafael", "Leslie", "Edgar",
					"Milton", "Raul", "Ben", "Chester", "Cecil", "Duane",
					"Franklin", "Andre", "Elmer", "Brad", "Gabriel", "Ron",
					"Mitchell", "Roland", "Arnold", "Harvey", "Jared",
					"Adrian", "Karl", "Cory", "Claude", "Erik", "Darryl",
					"Jamie", "Neil", "Jessie", "Christian", "Javier",
					"Fernando", "Clinton", "Ted", "Mathew", "Tyrone", "Darren",
					"Lonnie", "Lance", "Cody", "Julio", "Kelly", "Kurt",
					"Allan", "Mary", "Patricia", "Linda",
                    "Barbara", "Elizabeth", "Jennifer", "Maria", "Susan", "Margaret",
                    "Dorothy", "Lisa", "Nancy", "Karen", "Betty", "Helen", "Sandra",
                    "Donna", "Carol", "Ruth", "Sharon", "Michelle", "Laura", "Sarah",
                    "Kimberly", "Deborah", "Jessica", "Shirley", "Cynthia", "Angela",
                    "Melissa", "Brenda", "Amy", "Anna", "Rebecca", "Virginia",
                    "Kathleen", "Pamela", "Martha", "Debra", "Amanda", "Stephanie",
                    "Carolyn", "Christine", "Marie", "Janet", "Catherine", "Frances",
                    "Ann", "Joyce", "Diane", "Alice", "Julie", "Heather", "Teresa",
                    "Doris", "Gloria", "Evelyn", "Jean", "Cheryl", "Mildred",
                    "Katherine", "Joan", "Ashley", "Judith", "Rose", "Janice", "Kelly",
                    "Nicole", "Judy", "Christina", "Kathy", "Theresa", "Beverly",
                    "Denise", "Tammy", "Irene", "Jane", "Lori", "Rachel", "Marilyn",
                    "Andrea", "Kathryn", "Louise", "Sara", "Anne", "Jacqueline",
                    "Wanda", "Bonnie", "Julia", "Ruby", "Lois", "Tina", "Phyllis",
                    "Norma", "Paula", "Diana", "Annie", "Lillian", "Emily", "Robin",
                    "Peggy", "Crystal", "Gladys", "Rita", "Dawn", "Connie", "Florence",
                    "Tracy", "Edna", "Tiffany", "Carmen", "Rosa", "Cindy", "Grace",
                    "Wendy", "Victoria", "Edith", "Kim", "Sherry", "Sylvia",
                    "Josephine", "Thelma", "Shannon", "Sheila", "Ethel", "Ellen",
                    "Elaine", "Marjorie", "Carrie", "Charlotte", "Monica", "Esther",
                    "Pauline", "Emma", "Juanita", "Anita", "Rhonda", "Hazel", "Amber",
                    "Eva", "Debbie", "April", "Leslie", "Clara", "Lucille", "Jamie",
                    "Joanne", "Eleanor", "Valerie", "Danielle", "Megan", "Alicia",
                    "Suzanne", "Michele", "Gail", "Bertha", "Darlene", "Veronica",
                    "Jill", "Erin", "Geraldine", "Lauren", "Cathy", "Joann",
                    "Lorraine", "Lynn", "Sally", "Regina", "Erica", "Beatrice",
                    "Dolores", "Bernice", "Audrey", "Yvonne", "Annette", "June",
                    "Samantha", "Marion", "Dana", "Stacy", "Ana", "Renee", "Ida",
                    "Vivian", "Roberta", "Holly", "Brittany", "Melanie", "Loretta",
                    "Yolanda", "Jeanette", "Laurie", "Katie", "Kristen", "Vanessa",
                    "Alma", "Sue", "Elsie", "Beth", "Jeanne", "Vicki", "Carla", "Tara",
                    "Rosemary", "Eileen", "Terri", "Gertrude", "Lucy", "Tonya", "Ella",
                    "Stacey", "Wilma", "Gina", "Kristin", "Jessie", "Natalie", "Agnes",
                    "Vera", "Willie", "Charlene", "Bessie", "Delores", "Melinda",
                    "Pearl", "Arlene", "Maureen", "Colleen", "Allison", "Tamara",
                    "Joy", "Georgia", "Constance", "Lillie", "Claudia", "Jackie",
                    "Marcia", "Tanya", "Nellie", "Minnie", "Marlene", "Heidi",
                    "Glenda", "Lydia", "Viola", "Courtney", "Marian", "Stella",
                    "Caroline", "Dora", "Jo" };
			
			String[] lastNames = new String[] { "Smith", "Johnson", "Williams",
                    "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor",
                    "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin",
                    "Thompson", "Garcia", "Martinez", "Robinson", "Clark", "Rodriguez",
                    "Lewis", "Lee", "Walker", "Hall", "Allen", "Young", "Hernandez",
                    "King", "Wright", "Lopez", "Hill", "Scott", "Green", "Adams",
                    "Baker", "Gonzalez", "Nelson", "Carter", "Mitchell", "Perez",
                    "Roberts", "Turner", "Phillips", "Campbell", "Parker", "Evans",
                    "Edwards", "Collins", "Stewart", "Sanchez", "Morris", "Rogers",
                    "Reed", "Cook", "Morgan", "Bell", "Murphy", "Bailey", "Rivera",
                    "Cooper", "Richardson", "Cox", "Howard", "Ward", "Torres",
                    "Peterson", "Gray", "Ramirez", "James", "Watson", "Brooks",
                    "Kelly", "Sanders", "Price", "Bennett", "Wood", "Barnes", "Ross",
                    "Henderson", "Coleman", "Jenkins", "Perry", "Powell", "Long",
                    "Patterson", "Hughes", "Flores", "Washington", "Butler", "Simmons",
                    "Foster", "Gonzales", "Bryant", "Alexander", "Russell", "Griffin",
                    "Diaz", "Hayes", "Myers", "Ford", "Hamilton", "Graham", "Sullivan",
                    "Wallace", "Woods", "Cole", "West", "Jordan", "Owens", "Reynolds",
                    "Fisher", "Ellis", "Harrison", "Gibson", "McDonald", "Cruz",
                    "Marshall", "Ortiz", "Gomez", "Murray", "Freeman", "Wells", "Webb",
                    "Simpson", "Stevens", "Tucker", "Porter", "Hunter", "Hicks",
                    "Crawford", "Henry", "Boyd", "Mason", "Morales", "Kennedy",
                    "Warren", "Dixon", "Ramos", "Reyes", "Burns", "Gordon", "Shaw",
                    "Holmes", "Rice", "Robertson", "Hunt", "Black", "Daniels",
                    "Palmer", "Mills", "Nichols", "Grant", "Knight", "Ferguson",
                    "Rose", "Stone", "Hawkins", "Dunn", "Perkins", "Hudson", "Spencer",
                    "Gardner", "Stephens", "Payne", "Pierce", "Berry", "Matthews",
                    "Arnold", "Wagner", "Willis", "Ray", "Watkins", "Olson", "Carroll",
                    "Duncan", "Snyder", "Hart", "Cunningham", "Bradley", "Lane",
                    "Andrews", "Ruiz", "Harper", "Fox", "Riley", "Armstrong",
                    "Carpenter", "Weaver", "Greene", "Lawrence", "Elliott", "Chavez",
                    "Sims", "Austin", "Peters", "Kelley", "Franklin", "Lawson",
                    "Fields", "Gutierrez", "Ryan", "Schmidt", "Carr", "Vasquez",
                    "Castillo", "Wheeler", "Chapman", "Oliver", "Montgomery",
                    "Richards", "Williamson", "Johnston", "Banks", "Meyer", "Bishop",
                    "McCoy", "Howell", "Alvarez", "Morrison", "Hansen", "Fernandez",
                    "Garza", "Harvey", "Little", "Burton", "Stanley", "Nguyen",
                    "George", "Jacobs", "Reid", "Kim", "Fuller", "Lynch", "Dean",
                    "Gilbert", "Garrett", "Romero", "Welch", "Larson", "Frazier",
                    "Burke", "Hanson", "Day", "Mendoza", "Moreno", "Bowman", "Medina",
                    "Fowler" };
			
			String[] someBooks = {
				"Hadoop definitive guide",
				"Hadoop in Action",
				"MapReduce Programming",
				"Pig Programming",
				"Hive",
				"C",
				"C and data structures",
				"Data Structures",
				"Hive practices"
			};

			File file = new File(
					"/home/cloudera/Desktop/erricsson/books_large.xml");

			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}

			// true = append file
			FileWriter fileWritter = new FileWriter(
					"/home/cloudera/Desktop/erricsson/books_large.xml",
					true);
			BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
			bufferWritter.write("<books>\n");
			bufferWritter.flush();
			int i = 0;
			while (i < 3000000) {
			//while (i < 10) {
				bufferWritter.write("<book>\n");
				bufferWritter.flush();
				Random r = new Random();
				int fname_index = r.nextInt(names.length);
				int lname_index = r.nextInt(lastNames.length);
				
				String full_name = names[fname_index] + " " + lastNames[lname_index];
				String title = someBooks[r.nextInt(someBooks.length)];
				String isbn = UUID.randomUUID().toString();
				
				String bookData = "<author>" + full_name + "</author>\n<title>" + title + "</title>\n<isbn>" + isbn + "</isbn>\n";

				bufferWritter.write(bookData);
				bufferWritter.flush();
				System.out.println("Done: " + i);
				bufferWritter.write("</book>\n");
				bufferWritter.flush();
				i++;
			}
			bufferWritter.write("</books>");
			bufferWritter.flush();
			// bufferWritter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
