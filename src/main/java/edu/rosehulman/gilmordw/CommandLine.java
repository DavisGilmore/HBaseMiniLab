package edu.rosehulman.gilmordw;

import java.util.Scanner;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class CommandLine {

	public CommandLine() {

	}

	public void run() {

	}

	public void loop() {
		Scanner input = new Scanner(System.in);
		HBaseConnection conn = new HBaseConnection();
		boolean loop = true;
		while (loop) {
			System.out.println("Welcome to the library");
			String line = input.nextLine();
			line = line.toLowerCase().trim();
			if (line.equals("init")) {
				System.out.println("Initializing database tables...");
				conn.createTables();
			} else if (line.equals("add_book") || line.equals("add book")) {
				System.out.println("What is the title of the book?");
				String title = input.nextLine();
				System.out.println("What is the book isbn?");
				String isbn = input.nextLine();
				System.out.println("How many pages are in the book?");
				String numPages = input.nextLine();
				System.out.println("How many authors does the book have?");
				String authorNum = input.nextLine();
				int authNum = 0;
				try {
					authNum = Integer.parseInt(authorNum);
				} catch (NumberFormatException e) {
					System.out.println("The number of authors must be a number. Cancelling the operation");
					continue;
				}
				if (authNum <= 0) {
					System.out.println("The number of authors must be 1 or more");
					continue;
				}
				String[] authors = new String[authNum];
				for (int a = 0; a < authNum; a++) {
					System.out.println("Enter the name of author number " + (a + 1));
					authors[a] = input.nextLine();
				}
				conn.addBook(title, isbn, numPages, authNum, authors);
			} else if (line.equals("delete_book") || line.equals("delete book")) {
				System.out.println("Enter the isbn of the book you want to delete");
				String isbn = input.nextLine();
				conn.deleteBook(isbn);
			} else if (line.equals("edit_book") || line.equals("edit book")) {
				System.out.println("What is the isbn of the book you would like to edit?");
				String isbn = input.nextLine();
				System.out.println("What attribute would you like to edit? Options are title, num_pages, authors");
				String option = input.nextLine();
				option = option.toLowerCase().trim();
				if (option.equals("title")) {
					System.out.println("What would like to change the title to?");
					String title = input.nextLine();
					conn.editTitle(isbn, title);
				} else if (option.equals("num_pages")) {
					System.out.println("What would like to change the number of pages to?");
					String numPages = input.nextLine();
					conn.editNumPages(isbn, numPages);
				} else if (option.equals("authors")) {
					System.out.println("Would you like to add or remove and author? (a/r)");
					String add = input.nextLine();
					add = add.toLowerCase().trim();
					if (add.charAt(0) == 'a') {
						System.out.println("What is the name of the author you would like to add?");
						String author = input.nextLine();
						conn.addAuthor(isbn, author);
					} else if (add.charAt(0) == 'r') {
						System.out.println("What is the name of the author you would like to remove?");
						String author = input.nextLine();
						conn.removeAuthor(isbn, author);
					} else {
						System.out.println("That is not an option");
					}
				} else {
					System.out.println("That is not an option.");
				}
			} else if (line.equals("search_books") || line.equals("search books")) {
				System.out.println("What attribute would you like to search by? Options are isbn, title, or author");
				String option = input.nextLine();
				option = option.toLowerCase().trim();
				if (option.equals("isbn")) {
					System.out.println("What isbn are you searching for?");
					String isbn = input.nextLine();
					conn.searchIsbn(isbn);
				} else if (option.equals("title")) {
					System.out.println("What title are you searching for?");
					String title = input.nextLine();
					conn.searchTitle(title);
				} else if (option.equals("author")) {
					System.out.println("What author are you searching for?");
					String author = input.nextLine();
					conn.searchAuthor(author);
				} else {
					System.out.println("That is not an option");
				}
			} else if (line.equals("sort_books") || line.equals("sort books")) {
				System.out.println("What attribute would you like to sort by? Options are isbn, title, or num_pages");
				String option = input.nextLine();
				option = option.toLowerCase().trim();
				if (option.equals("isbn")) {
					conn.sortIsbn();
				} else if (option.equals("title")) {
					conn.sortTitle();
				} else if (option.equals("num_pages")) {
					conn.sortNumPages();
				} else {
					System.out.println("That is not an option");
				}
			} else if (line.equals("add_borrower") || line.equals("add borrower")) {
				System.out.println("What is the name of the borrower?");
				String name = input.nextLine();
				System.out.println("What is the username of the borrower?");
				String username = input.nextLine();
				System.out.println("What is the phone number of the borrower?");
				String phone = input.nextLine();
				conn.addBorrower(username, name, phone);
			} else if (line.equals("delete_borrower") || line.equals("delete borrower")) {
				System.out.println("What is the username of the borrower to be deleted?");
				String username = input.nextLine();
				conn.deleteBorrower(username);
			} else if (line.equals("edit_borrower") || line.equals("edit borrower")) {
				System.out.println("What is the username of the borrower you would like to edit?");
				String username = input.nextLine();
				System.out.println("What attribute would you like to edit? Options are name and phone");
				String option = input.nextLine();
				option = option.toLowerCase().trim();
				if (option.equals("name")) {
					System.out.println("What is the new name?");
					String name = input.nextLine();
					conn.editName(username, name);
				} else if (option.equals("phone")) {
					System.out.println("What is the new phone?");
					String phone = input.nextLine();
					conn.editPhone(username, phone);
				} else {
					System.out.println("That is not an option");
				}
			} else if (line.equals("search_borrowers") || line.equals("search borrowers")) {
				System.out.println("What attribute would you like to search by? Options are username, name");
				String option = input.nextLine();
				if (option.equals("username")) {
					System.out.println("What username are you searching for?");
					String username = input.nextLine();
					conn.searchUsername(username);
				} else if (option.equals("name")) {
					System.out.println("What name are you searching for?");
					String name = input.nextLine();
					conn.searchName(name);
				} else {
					System.out.println("That is not an option");
				}
			} else if (line.equals("checkout")) {
				System.out.println("What is the username of the borrower?");
				String username = input.nextLine();
				System.out.println("What is the isbn of the book?");
				String isbn = input.nextLine();
				conn.checkout(username, isbn);
			} else if (line.equals("checkin")) {
				System.out.println("What is the isbn of the book?");
				String isbn = input.nextLine();
				conn.checkin(isbn);
			} else if (line.equals("book_status") || line.equals("book status")) {
				System.out.println("What is the isbn of the book?");
				String isbn = input.nextLine();
				conn.bookStatus(isbn);
			} else if (line.equals("books_borrowed") || line.equals("books borrowed")) {
				System.out.println("What is the username of the borrower?");
				String username = input.nextLine();
				conn.numBooks(username);
			} else {
				System.out.println("hint");
			}
		}
	}

	public static void main(String[] args) {
		initLogger();
		CommandLine cmd = new CommandLine();
		cmd.loop();
	}

	public static void initLogger() {
		PatternLayout layout = new PatternLayout("%-5p %d %m%n");
		FileAppender appender = new FileAppender();
		appender.setName("PlayBall");
		appender.setFile("playBall.log");
		appender.activateOptions();
		appender.setLayout(layout);
		Logger.getRootLogger().addAppender(appender);
	}
}
