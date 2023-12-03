package bigdata;

public class Book {
	
	private String title;
	private String author;
	private double score;
	private int ratings;
	private int published;
	private String description;
	private String genre;
	
	public Book(String title, String author, double score, int ratings, int published, String description,
			String genre) {
		
		this.title = title;
		this.author = author;
		this.score = score;
		this.ratings = ratings;
		this.published = published;
		this.description = description;
		this.genre = genre;
	}
	
	

	public Book() {super();
		// TODO Auto-generated constructor stub
	}



	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public int getRatings() {
		return ratings;
	}

	public void setRatings(int ratings) {
		this.ratings = ratings;
	}

	public int getPublished() {
		return published;
	}

	public void setPublished(int published) {
		this.published = published;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getGenre() {
		return genre;
	}

	public void setGenre(String genre) {
		this.genre = genre;
	}

	@Override
	public String toString() {
		return "Book [title=" + title + ", author=" + author + ", score=" + score + ", ratings=" + ratings
				+ ", published=" + published + ", description=" + description + ", genre=" + genre + "]";
	}
	
	
	
	
	

}
