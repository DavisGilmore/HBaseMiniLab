package edu.rosehulman.gilmordw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseConnection {

	private Connection conn;
	private Configuration config;
	private HBaseAdmin admin;

	public HBaseConnection() {
		System.out.println("Connecting to HBase");
		config = HBaseConfiguration.create();
		config.addResource("hbase-site.xml");
		config.set("zookeeper.znode.parent", "/hbase-unsecure");

		System.out.println("Creating connection");
		try {
			conn = ConnectionFactory.createConnection(config);
			admin = (HBaseAdmin) conn.getAdmin();

			System.out.println("Connection made");
		} catch (IOException e) {
			System.out.println("IOException during connection");
		}

	}

	public void createTables() {
		try {
			HTableDescriptor bookIsbn = new HTableDescriptor(TableName.valueOf("book_isbn"));
			bookIsbn.addFamily(new HColumnDescriptor("book"));
			admin.createTable(bookIsbn);

			HTableDescriptor bookTitle = new HTableDescriptor(TableName.valueOf("book_title"));
			bookIsbn.addFamily(new HColumnDescriptor("book"));
			admin.createTable(bookTitle);

			HTableDescriptor bookNumPages = new HTableDescriptor(TableName.valueOf("book_isbn"));
			bookIsbn.addFamily(new HColumnDescriptor("book"));
			admin.createTable(bookNumPages);

			HTableDescriptor borrowerUsername = new HTableDescriptor(TableName.valueOf("borrower_username"));
			borrowerUsername.addFamily(new HColumnDescriptor("borrower"));
			admin.createTable(borrowerUsername);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void addBook(String title, String isbn, String numPages, int authNum, String[] authors) {
		try {
			Table table = conn.getTable(TableName.valueOf("book_isbn"));
			Table tableTitle = conn.getTable(TableName.valueOf("book_title"));
			Table tableNumPages = conn.getTable(TableName.valueOf("book_num_pages"));

			Get get = new Get(Bytes.toBytes(isbn));
			get.addColumn(Bytes.toBytes("book"), Bytes.toBytes("title"));
			Result result = table.get(get);
			if (!result.isEmpty()) {
				System.out.println("Isbn already exists");
				return;
			}

			Put put = new Put(Bytes.toBytes(isbn));
			put.addColumn(Bytes.toBytes("book"), Bytes.toBytes("isbn"), Bytes.toBytes(isbn));
			put.addColumn(Bytes.toBytes("book"), Bytes.toBytes("title"), Bytes.toBytes(title));
			put.addColumn(Bytes.toBytes("book"), Bytes.toBytes("num_pages"), Bytes.toBytes(numPages));
			String appendedAuthors = "";
			for (int a = 0; a < authNum; a++) {
				appendedAuthors += authors[a] + ",";
			}
			put.addColumn(Bytes.toBytes("book"), Bytes.toBytes("authors"),
					Bytes.toBytes(appendedAuthors.substring(0, appendedAuthors.length() - 1)));

			table.put(put);

			Put putTitle = new Put(Bytes.toBytes(title + "+" + isbn));
			putTitle.addColumn(Bytes.toBytes("book"), Bytes.toBytes("isbn"), Bytes.toBytes(isbn));
			putTitle.addColumn(Bytes.toBytes("book"), Bytes.toBytes("title"), Bytes.toBytes(title));
			putTitle.addColumn(Bytes.toBytes("book"), Bytes.toBytes("num_pages"), Bytes.toBytes(numPages));
			putTitle.addColumn(Bytes.toBytes("book"), Bytes.toBytes("authors"),
					Bytes.toBytes(appendedAuthors.substring(0, appendedAuthors.length() - 1)));

			tableTitle.put(putTitle);

			Put putPages = new Put(Bytes.toBytes(numPages + "+" + isbn));
			putPages.addColumn(Bytes.toBytes("book"), Bytes.toBytes("isbn"), Bytes.toBytes(isbn));
			putPages.addColumn(Bytes.toBytes("book"), Bytes.toBytes("title"), Bytes.toBytes(title));
			putPages.addColumn(Bytes.toBytes("book"), Bytes.toBytes("num_pages"), Bytes.toBytes(numPages));
			putPages.addColumn(Bytes.toBytes("book"), Bytes.toBytes("authors"),
					Bytes.toBytes(appendedAuthors.substring(0, appendedAuthors.length() - 1)));

			tableNumPages.put(putPages);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void deleteBook(String isbn) {
		if (!checkBookExists(isbn)) {
			System.out.println("Book does not exist");
			return;
		}
		if (isBookCheckedOut(isbn)) {
			System.out.println("The book is checked out");
			return;
		}
		try {
			Table tableIsbn = conn.getTable(TableName.valueOf("book_isbn"));
			

			Get get = new Get(Bytes.toBytes(isbn));
			get.addColumn(Bytes.toBytes("book"), Bytes.toBytes("title"));
			get.addColumn(Bytes.toBytes("book"), Bytes.toBytes("num_pages"));
			Result result = tableIsbn.get(get);
			String title = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("title")));
			String num_pages = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("num_pages")));

			Table tableTitle = conn.getTable(TableName.valueOf("book_title"));
			Delete delTitle = new Delete(Bytes.toBytes(title + "+" + isbn));
			tableTitle.delete(delTitle);

			Table tableNumPages = conn.getTable(TableName.valueOf("book_num_pages"));
			Delete delNumPages = new Delete(Bytes.toBytes(num_pages + "+" + isbn));
			tableNumPages.delete(delNumPages);
			Delete delIsbn = new Delete(Bytes.toBytes(isbn));
			tableIsbn.delete(delIsbn);

		} catch (IOException e) {
			System.out.println("IO Exception during delete");
		}

	}

	public void editTitle(String isbn, String title) {
		if (!checkBookExists(isbn)) {
			System.out.println("Book does not exist");
			return;
		}
		try {
			Table tableIsbn = conn.getTable(TableName.valueOf("book_isbn"));
			Table tableTitle = conn.getTable(TableName.valueOf("book_title"));
			Table tableNumPages = conn.getTable(TableName.valueOf("book_num_pages"));

			Get get = new Get(Bytes.toBytes(isbn));
			get.addColumn(Bytes.toBytes("book"), Bytes.toBytes("title"));
			get.addColumn(Bytes.toBytes("book"), Bytes.toBytes("num_pages"));
			get.addColumn(Bytes.toBytes("book"), Bytes.toBytes("authors"));
			Result result = tableIsbn.get(get);
			String old_title = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("title")));
			String num_pages = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("num_pages")));
			String authList = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("authors")));

			Put put = new Put(Bytes.toBytes(isbn));
			put.addColumn(Bytes.toBytes("book"), Bytes.toBytes("title"), Bytes.toBytes(title));

			tableIsbn.put(put);

			Put putNumPages = new Put(Bytes.toBytes(num_pages + "+" + isbn));
			putNumPages.addColumn(Bytes.toBytes("book"), Bytes.toBytes("title"), Bytes.toBytes(title));

			tableNumPages.put(putNumPages);

			Delete delete = new Delete(Bytes.toBytes(old_title + "+" + isbn));
			tableTitle.delete(delete);

			Put putTitle = new Put(Bytes.toBytes(title + "+" + isbn));
			putTitle.addColumn(Bytes.toBytes("book"), Bytes.toBytes("isbn"), Bytes.toBytes(isbn));
			putTitle.addColumn(Bytes.toBytes("book"), Bytes.toBytes("title"), Bytes.toBytes(title));
			putTitle.addColumn(Bytes.toBytes("book"), Bytes.toBytes("num_pages"), Bytes.toBytes(num_pages));
			putTitle.addColumn(Bytes.toBytes("book"), Bytes.toBytes("authors"), Bytes.toBytes(authList));

			tableTitle.put(putTitle);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void editNumPages(String isbn, String numPages) {
		if (!checkBookExists(isbn)) {
			System.out.println("Book does not exist");
			return;
		}
		try {
			Table tableIsbn = conn.getTable(TableName.valueOf("book_isbn"));
			Table tableTitle = conn.getTable(TableName.valueOf("book_title"));
			Table tableNumPages = conn.getTable(TableName.valueOf("book_num_pages"));

			Get get = new Get(Bytes.toBytes(isbn));
			get.addColumn(Bytes.toBytes("book"), Bytes.toBytes("title"));
			get.addColumn(Bytes.toBytes("book"), Bytes.toBytes("num_pages"));
			get.addColumn(Bytes.toBytes("book"), Bytes.toBytes("authors"));
			Result result = tableIsbn.get(get);
			String title = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("title")));
			String old_num_pages = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("num_pages")));
			String authList = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("authors")));

			Put put = new Put(Bytes.toBytes(isbn));
			put.addColumn(Bytes.toBytes("book"), Bytes.toBytes("num_pages"), Bytes.toBytes(numPages));

			tableIsbn.put(put);

			Put putTitle = new Put(Bytes.toBytes(title + "+" + isbn));
			putTitle.addColumn(Bytes.toBytes("book"), Bytes.toBytes("num_pages"), Bytes.toBytes(numPages));

			tableTitle.put(putTitle);

			Delete delete = new Delete(Bytes.toBytes(old_num_pages + "+" + isbn));
			tableNumPages.delete(delete);

			Put putNumPages = new Put(Bytes.toBytes(numPages + "+" + isbn));
			putNumPages.addColumn(Bytes.toBytes("book"), Bytes.toBytes("isbn"), Bytes.toBytes(isbn));
			putNumPages.addColumn(Bytes.toBytes("book"), Bytes.toBytes("title"), Bytes.toBytes(title));
			putNumPages.addColumn(Bytes.toBytes("book"), Bytes.toBytes("num_pages"), Bytes.toBytes(numPages));
			putNumPages.addColumn(Bytes.toBytes("book"), Bytes.toBytes("authors"), Bytes.toBytes(authList));

			tableNumPages.put(putNumPages);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void addAuthor(String isbn, String author) {
		if (!checkBookExists(isbn)) {
			System.out.println("Book does not exist");
			return;
		}
		try {
			Table table = conn.getTable(TableName.valueOf("book_isbn"));
			Table tableTitle = conn.getTable(TableName.valueOf("book_title"));
			Table tableNumPages = conn.getTable(TableName.valueOf("book_num_pages"));

			Get get = new Get(Bytes.toBytes(isbn));
			get.addColumn(Bytes.toBytes("book"), Bytes.toBytes("title"));
			get.addColumn(Bytes.toBytes("book"), Bytes.toBytes("num_pages"));
			get.addColumn(Bytes.toBytes("book"), Bytes.toBytes("authors"));
			Result result = table.get(get);
			String title = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("title")));
			String num_pages = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("num_pages")));
			String authors = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("authors")));
			authors += "," + author;

			Put put = new Put(Bytes.toBytes(isbn));
			put.addColumn(Bytes.toBytes("book"), Bytes.toBytes("authors"), Bytes.toBytes(authors));
			table.put(put);

			Put putTitle = new Put(Bytes.toBytes(title + "+" + isbn));
			put.addColumn(Bytes.toBytes("book"), Bytes.toBytes("authors"), Bytes.toBytes(authors));
			tableTitle.put(putTitle);

			Put putNumPages = new Put(Bytes.toBytes(num_pages + "+" + isbn));
			putNumPages.addColumn(Bytes.toBytes("book"), Bytes.toBytes("authors"), Bytes.toBytes(authors));
			tableNumPages.put(putNumPages);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void removeAuthor(String isbn, String author) {
		if (!checkBookExists(isbn)) {
			System.out.println("Book does not exist");
			return;
		}
		try {
			Table table = conn.getTable(TableName.valueOf("book_isbn"));
			Table tableTitle = conn.getTable(TableName.valueOf("book_title"));
			Table tableNumPages = conn.getTable(TableName.valueOf("book_num_pages"));

			Get get = new Get(Bytes.toBytes(isbn));
			get.addColumn(Bytes.toBytes("book"), Bytes.toBytes("title"));
			get.addColumn(Bytes.toBytes("book"), Bytes.toBytes("num_pages"));
			get.addColumn(Bytes.toBytes("book"), Bytes.toBytes("authors"));
			Result result = table.get(get);
			String title = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("title")));
			String num_pages = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("num_pages")));
			String authors = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("authors")));
			String[] authorsArray = authors.split(",");
			String newAuthors = "";
			for (String auth : authorsArray) {
				if (auth.equals(author)) {
					continue;
				}
				newAuthors += auth + ",";
			}
			if (!newAuthors.isEmpty()) {
				newAuthors = newAuthors.substring(0, newAuthors.length() - 1);
			}

			Put put = new Put(Bytes.toBytes(isbn));
			put.addColumn(Bytes.toBytes("book"), Bytes.toBytes("authors"), Bytes.toBytes(newAuthors));
			table.put(put);

			Put putTitle = new Put(Bytes.toBytes(title + "+" + isbn));
			put.addColumn(Bytes.toBytes("book"), Bytes.toBytes("authors"), Bytes.toBytes(newAuthors));
			tableTitle.put(putTitle);

			Put putNumPages = new Put(Bytes.toBytes(num_pages + "+" + isbn));
			putNumPages.addColumn(Bytes.toBytes("book"), Bytes.toBytes("authors"), Bytes.toBytes(newAuthors));
			tableNumPages.put(putNumPages);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void searchIsbn(String isbn) {
		try {
			Table table = conn.getTable(TableName.valueOf("book_isbn"));

			Get get = new Get(Bytes.toBytes(isbn));
			get.addColumn(Bytes.toBytes("book"), Bytes.toBytes("title"));
			get.addColumn(Bytes.toBytes("book"), Bytes.toBytes("num_pages"));
			get.addColumn(Bytes.toBytes("book"), Bytes.toBytes("authors"));

			Result result = table.get(get);
			String title = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("title")));
			String num_pages = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("num_pages")));
			String authList = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("authors")));
			authList = authList.replace(",", " , ");
			System.out.println("ISBN: " + isbn + ", Title: " + title + ", Number of Pages: " + num_pages
					+ ", Author(s): " + authList);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void searchTitle(String title) {
		try {
			Table table = conn.getTable(TableName.valueOf("book_title"));

			Scan scan = new Scan(Bytes.toBytes(title), Bytes.toBytes(title + Character.MAX_VALUE));
			scan.addColumn(Bytes.toBytes("book"), Bytes.toBytes("isbn"));
			scan.addColumn(Bytes.toBytes("book"), Bytes.toBytes("num_pages"));
			scan.addColumn(Bytes.toBytes("book"), Bytes.toBytes("authors"));

			ResultScanner results = table.getScanner(scan);
			for (Result result : results) {
				String isbn = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("isbn")));
				String num_pages = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("num_pages")));
				String authList = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("authors")));
				authList = authList.replace(",", " , ");
				System.out.println("ISBN: " + isbn + ", Title: " + title + ", Number of Pages: " + num_pages
						+ ", Author(s): " + authList);
			}

		} catch (IOException e) {
			System.out.println("IOException in search title");
		}

	}

	public void searchAuthor(String author) {
		try {
			Table table = conn.getTable(TableName.valueOf("book_isbn"));

			FilterList filterList = new FilterList(Operator.MUST_PASS_ONE);
			SingleColumnValueFilter scvf = new SingleColumnValueFilter(Bytes.toBytes("book"), Bytes.toBytes("authors"),
					CompareOp.EQUAL, new SubstringComparator(author));
			filterList.addFilter(scvf);
			Scan scan = new Scan();
			scan.setFilter(filterList);

			scan.addColumn(Bytes.toBytes("book"), Bytes.toBytes("isbn"));
			scan.addColumn(Bytes.toBytes("book"), Bytes.toBytes("title"));
			scan.addColumn(Bytes.toBytes("book"), Bytes.toBytes("num_pages"));
			scan.addColumn(Bytes.toBytes("book"), Bytes.toBytes("authors"));

			ResultScanner results = table.getScanner(scan);
			for (Result result : results) {
				String isbn = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("isbn")));
				String title = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("title")));
				String num_pages = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("num_pages")));
				String authList = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("authors")));
				authList = authList.replace(",", " , ");
				System.out.println("ISBN: " + isbn + ", Title: " + title + ", Number of Pages: " + num_pages
						+ ", Author(s): " + authList);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void sortIsbn() {
		try {
			Table table = conn.getTable(TableName.valueOf("book_isbn"));

			Scan scan = new Scan();
			scan.addColumn(Bytes.toBytes("book"), Bytes.toBytes("isbn"));
			scan.addColumn(Bytes.toBytes("book"), Bytes.toBytes("title"));
			scan.addColumn(Bytes.toBytes("book"), Bytes.toBytes("num_pages"));
			scan.addColumn(Bytes.toBytes("book"), Bytes.toBytes("authors"));

			ResultScanner results = table.getScanner(scan);
			for (Result result : results) {
				String isbn = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("isbn")));
				String title = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("title")));
				String num_pages = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("num_pages")));
				String authList = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("authors")));
				authList = authList.replace(",", " , ");
				System.out.println("ISBN: " + isbn + ", Title: " + title + ", Number of Pages: " + num_pages
						+ ", Author(s): " + authList);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void sortTitle() {
		try {
			Table table = conn.getTable(TableName.valueOf("book_title"));

			Scan scan = new Scan();
			scan.addColumn(Bytes.toBytes("book"), Bytes.toBytes("isbn"));
			scan.addColumn(Bytes.toBytes("book"), Bytes.toBytes("title"));
			scan.addColumn(Bytes.toBytes("book"), Bytes.toBytes("num_pages"));
			scan.addColumn(Bytes.toBytes("book"), Bytes.toBytes("authors"));

			ResultScanner results = table.getScanner(scan);
			for (Result result : results) {
				String isbn = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("isbn")));
				String title = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("title")));
				String num_pages = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("num_pages")));
				String authList = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("authors")));
				authList = authList.replace(",", " , ");
				System.out.println("ISBN: " + isbn + ", Title: " + title + ", Number of Pages: " + num_pages
						+ ", Author(s): " + authList);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void sortNumPages() {
		try {
			Table table = conn.getTable(TableName.valueOf("book_num_pages"));

			Scan scan = new Scan();
			scan.addColumn(Bytes.toBytes("book"), Bytes.toBytes("isbn"));
			scan.addColumn(Bytes.toBytes("book"), Bytes.toBytes("title"));
			scan.addColumn(Bytes.toBytes("book"), Bytes.toBytes("num_pages"));
			scan.addColumn(Bytes.toBytes("book"), Bytes.toBytes("authors"));

			ResultScanner results = table.getScanner(scan);
			for (Result result : results) {
				String isbn = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("isbn")));
				String title = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("title")));
				String num_pages = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("num_pages")));
				String authList = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("authors")));
				authList = authList.replace(",", " , ");
				System.out.println("ISBN: " + isbn + ", Title: " + title + ", Number of Pages: " + num_pages
						+ ", Author(s): " + authList);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void addBorrower(String username, String name, String phone) {
		try {
			Table borrower = conn.getTable(TableName.valueOf("borrower_username"));

			Put put = new Put(Bytes.toBytes(username));
			put.addColumn(Bytes.toBytes("borrower"), Bytes.toBytes("username"), Bytes.toBytes(username));
			put.addColumn(Bytes.toBytes("borrower"), Bytes.toBytes("name"), Bytes.toBytes(name));
			put.addColumn(Bytes.toBytes("borrower"), Bytes.toBytes("phone"), Bytes.toBytes(phone));
			put.addColumn(Bytes.toBytes("borrower"), Bytes.toBytes("books_num"), Bytes.toBytes("" + 0));

			borrower.put(put);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void deleteBorrower(String username) {
		if (!checkUserExists(username)) {
			System.out.println("Borrower does not exist");
			return;
		}
		try {
			Table borrower = conn.getTable(TableName.valueOf("borrower_username"));

			Get get = new Get(Bytes.toBytes(username));
			get.addColumn(Bytes.toBytes("borrower"), Bytes.toBytes("books_num"));
			Result result = borrower.get(get);
			int books = Integer
					.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("borrower"), Bytes.toBytes("books_num"))));

			if (books > 0) {
				System.out.println("The Borrower has books checked out");
				return;
			}

			Delete delete = new Delete(Bytes.toBytes(username));

			borrower.delete(delete);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void editName(String username, String name) {
		if (!checkUserExists(username)) {
			System.out.println("Borrower does not exist");
			return;
		}
		try {
			Table borrower = conn.getTable(TableName.valueOf("borrower_username"));

			Put put = new Put(Bytes.toBytes(username));
			put.addColumn(Bytes.toBytes("borrower"), Bytes.toBytes("name"), Bytes.toBytes(name));

			borrower.put(put);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void editPhone(String username, String phone) {
		if (!checkUserExists(username)) {
			System.out.println("Borrower does not exist");
			return;
		}
		try {
			Table borrower = conn.getTable(TableName.valueOf("borrower_username"));

			Put put = new Put(Bytes.toBytes(username));
			put.addColumn(Bytes.toBytes("borrower"), Bytes.toBytes("phone"), Bytes.toBytes(phone));

			borrower.put(put);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void searchUsername(String username) {
		if (!checkUserExists(username)) {
			System.out.println("Borrower does not exist");
			return;
		}
		try {
			Table borrower = conn.getTable(TableName.valueOf("borrower_username"));

			Get get = new Get(Bytes.toBytes(username));
			get.addColumn(Bytes.toBytes("borrower"), Bytes.toBytes("username"));
			get.addColumn(Bytes.toBytes("borrower"), Bytes.toBytes("name"));
			get.addColumn(Bytes.toBytes("borrower"), Bytes.toBytes("phone"));
			Result result = borrower.get(get);
			String name = Bytes.toString(result.getValue(Bytes.toBytes("borrower"), Bytes.toBytes("name")));
			String phone = Bytes.toString(result.getValue(Bytes.toBytes("borrower"), Bytes.toBytes("phone")));

			System.out.println("Username: " + username + ", Name: " + name + ", Phone: " + phone);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void searchName(String name) {
		try {
			Table table = conn.getTable(TableName.valueOf("borrower_username"));

			FilterList filterList = new FilterList(Operator.MUST_PASS_ONE);
			SingleColumnValueFilter scvf = new SingleColumnValueFilter(Bytes.toBytes("borrower"), Bytes.toBytes("name"),
					CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(name)));
			filterList.addFilter(scvf);
			Scan scan = new Scan();
			scan.setFilter(filterList);

			scan.addColumn(Bytes.toBytes("borrower"), Bytes.toBytes("username"));
			scan.addColumn(Bytes.toBytes("borrower"), Bytes.toBytes("name"));
			scan.addColumn(Bytes.toBytes("borrower"), Bytes.toBytes("phone"));

			ResultScanner results = table.getScanner(scan);
			for (Result result : results) {
				String username = Bytes.toString(result.getValue(Bytes.toBytes("borrower"), Bytes.toBytes("username")));
				String phone = Bytes.toString(result.getValue(Bytes.toBytes("borrower"), Bytes.toBytes("phone")));

				System.out.println("Username: " + username + ", Name: " + name + ", Phone: " + phone);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void checkout(String username, String isbn) {
		if (!checkUserExists(username)) {
			System.out.println("Borrower does not exist");
			return;
		}
		if (!checkBookExists(isbn)) {
			System.out.println("Book does not exist");
			return;
		}
		if (isBookCheckedOut(isbn)) {
			System.out.println("The book is checked out");
			return;
		}
		try {
			Table book = conn.getTable(TableName.valueOf("book_isbn"));
			Table borrower = conn.getTable(TableName.valueOf("borrower_username"));

			Get get = new Get(Bytes.toBytes(username));
			get.addColumn(Bytes.toBytes("borrower"), Bytes.toBytes("books_num"));
			Result result = borrower.get(get);
			int books = Integer
					.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("borrower"), Bytes.toBytes("books_num"))));

			Put putBook = new Put(Bytes.toBytes(isbn));
			putBook.addColumn(Bytes.toBytes("book"), Bytes.toBytes("borrower"), Bytes.toBytes(username));

			book.put(putBook);

			Put putBorrower = new Put(Bytes.toBytes(username));
			putBorrower.addColumn(Bytes.toBytes("borrower"), Bytes.toBytes("books_num"),
					Bytes.toBytes("" + (books + 1)));

			borrower.put(putBorrower);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void checkin(String isbn) {
		if (!checkBookExists(isbn)) {
			System.out.println("Book does not exist");
			return;
		}
		if (!isBookCheckedOut(isbn)) {
			System.out.println("The book is not checked out");
			return;
		}
		try {
			Table book = conn.getTable(TableName.valueOf("book_isbn"));

			Get getBook = new Get(Bytes.toBytes(isbn));
			getBook.addColumn(Bytes.toBytes("book"), Bytes.toBytes("borrower"));
			Result resultBook = book.get(getBook);
			String username = Bytes.toString(resultBook.getValue(Bytes.toBytes("book"), Bytes.toBytes("borrower")));

			Delete delete = new Delete(Bytes.toBytes(isbn));
			delete.addColumn(Bytes.toBytes("book"), Bytes.toBytes("borrower"));
			book.delete(delete);

			Table borrower = conn.getTable(TableName.valueOf("borrower_username"));

			Get getBorrower = new Get(Bytes.toBytes(username));
			getBorrower.addColumn(Bytes.toBytes("borrower"), Bytes.toBytes("books_num"));
			Result resultBorrower = borrower.get(getBorrower);
			int books = Integer.parseInt(
					Bytes.toString(resultBorrower.getValue(Bytes.toBytes("borrower"), Bytes.toBytes("books_num"))));

			Put putBorrower = new Put(Bytes.toBytes(username));
			putBorrower.addColumn(Bytes.toBytes("borrower"), Bytes.toBytes("books_num"),
					Bytes.toBytes("" + (books - 1)));

			borrower.put(putBorrower);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void numBooks(String username) {
		if (!checkUserExists(username)) {
			System.out.println("Borrower does not exist");
			return;
		}
		try {
			Table borrower = conn.getTable(TableName.valueOf("borrower_username"));

			Get get = new Get(Bytes.toBytes(username));
			get.addColumn(Bytes.toBytes("borrower"), Bytes.toBytes("books_num"));
			Result result = borrower.get(get);
			int books = Integer
					.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("borrower"), Bytes.toBytes("books_num"))));
			System.out.println("The user has checked out " + books + " book(s)");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void bookStatus(String isbn) {
		if (!checkBookExists(isbn)) {
			System.out.println("Book does not exist");
			return;
		}
		if (!isBookCheckedOut(isbn)) {
			System.out.println("The book is not checked out");
			return;
		}
		try {
			Table book = conn.getTable(TableName.valueOf("book_isbn"));

			Get get = new Get(Bytes.toBytes(isbn));
			get.addColumn(Bytes.toBytes("book"), Bytes.toBytes("borrower"));
			Result result = book.get(get);
			String borrower = Bytes.toString(result.getValue(Bytes.toBytes("book"), Bytes.toBytes("borrower")));
			System.out.println("The book has been checked out to " + borrower);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private boolean checkBookExists(String isbn) {
		try {
			Table book = conn.getTable(TableName.valueOf("book_isbn"));

			Get get = new Get(Bytes.toBytes(isbn));
			get.addColumn(Bytes.toBytes("book"), Bytes.toBytes("isbn"));
			Result result = book.get(get);
			if (result.isEmpty())
				return false;
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	private boolean checkUserExists(String username) {
		try {
			Table borrower = conn.getTable(TableName.valueOf("borrower_username"));

			Get get = new Get(Bytes.toBytes(username));
			get.addColumn(Bytes.toBytes("borrower"), Bytes.toBytes("username"));
			Result result = borrower.get(get);
			if (result.isEmpty())
				return false;
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	private boolean isBookCheckedOut(String isbn) {
		try {
			Table book = conn.getTable(TableName.valueOf("book_isbn"));

			Get get = new Get(Bytes.toBytes(isbn));
			get.addColumn(Bytes.toBytes("book"), Bytes.toBytes("borrower"));
			Result result = book.get(get);
			if (result.isEmpty()) {
				return false;
			}
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
}
