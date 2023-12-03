package bigdata;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Driver {

	public static void main(String[] args) throws FileNotFoundException {
		// TODO Auto-generated method stub
		
		List<Book> books=new LinkedList<>();
		
		File f= new File("input.txt");
		Scanner sc=new Scanner(f);
		while(sc.hasNextLine()) {
			String ch[]=sc.nextLine().split("\t");
				Book b=new Book();
				b.setTitle(ch[0]);
				b.setAuthor(ch[1]);
				b.setScore(Double.parseDouble(ch[2]));
				b.setRatings(Integer.parseInt(ch[3]));
				b.setPublished(Integer.parseInt(ch[4]));
				b.setDescription(ch[5]);
				b.setGenre(ch[6]);
				books.add(b);
			
		}
		
		
//		1.Use Java Streams to count the total number of ratings for all books.
		int totalRatings = books.stream()
                .mapToInt(Book::getRatings)
                .sum();
		System.out.println("Total number of ratings: " + totalRatings);
		
//		2.Use Java Streams to identify the author with the highest total score across all their books.
		
		// Calculate total score per author using streams
        Map<String, Double> authorTotalScoreMap = books.stream()
                .collect(Collectors.groupingBy(Book::getAuthor, Collectors.summingDouble(Book::getScore)));

        // Identify the author with the highest total score
        Map.Entry<String, Double> maxAuthorEntry = authorTotalScoreMap.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .orElse(null);

        if (maxAuthorEntry != null) {
            System.out.println("Author with the highest total score: " + maxAuthorEntry.getKey() +
                    " (Total Score: " + maxAuthorEntry.getValue() + ")");
        } else {
            System.out.println("No books found");
        }
        
        
//       3.  Use Java Streams to find the authors who had written more than one book
        Map<String, Long> authorsWithMultipleBooks = books.stream()
                .collect(Collectors.groupingBy(Book::getAuthor, Collectors.counting()))
                .entrySet().stream()
                .filter(entry -> entry.getValue() > 1)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        System.out.println("Authors with more than one book: " + authorsWithMultipleBooks);
        
        
//       4.  use Java Streams to find the book in every year which is having high score
        Map<Integer, Book> highestScoreBooksByYear = books.stream()
                .collect(Collectors.groupingBy(Book::getPublished,
                        Collectors.collectingAndThen(
                                Collectors.maxBy(Comparator.comparingDouble(Book::getScore)),
                                Optional::orElseThrow
                        )));

        System.out.println("Book with highest score each year: ");
        Collection<Book> b = highestScoreBooksByYear.values();
        b.stream().forEach(e->System.out.println(e.getPublished()+":"+e.getTitle()));;
        
        
        //5 Measure and compare the time it takes to calculate the total number of
//        ratings using Java Streams versus parallel streams. Assess the performance improvement achieved with parallelism.
//        
        long startTime = System.nanoTime();

        // Calculate total number of ratings using streams
        int ratings = books.parallelStream()
                .mapToInt(Book::getRatings)
                .sum();

        long endTime = System.nanoTime();

        long timeElapsed = endTime - startTime;
        System.out.println("Total number of ratings: " + totalRatings);
        System.out.println("Time taken with Java Streams: " + TimeUnit.NANOSECONDS.toMillis(timeElapsed) + " ms");
        
        
        
        startTime = System.nanoTime();

        // Calculate total number of ratings using streams
         ratings = books.stream()
                .mapToInt(Book::getRatings)
                .sum();

        endTime = System.nanoTime();

         timeElapsed = endTime - startTime;
        System.out.println("Total number of ratings: " + totalRatings);
        System.out.println("Time taken with Java Streams: " + TimeUnit.NANOSECONDS.toMillis(timeElapsed) + " ms");
        
        
        
        //6
        long startTimeSequential = System.currentTimeMillis();
        List<Book> booksPublishedIn2020Parallel = books.stream()
        	    .filter(book -> book.getPublished() == 2020)
        	    .collect(Collectors.toList());
        long endTimeSequential = System.currentTimeMillis();

        // Print results for sequential stream
        //System.out.println("Books published in 2020 (Sequential): " + booksPublishedIn2020Parallel);
        System.out.println("Time taken (Sequential): " + (endTimeSequential - startTimeSequential) + " ms");

        // Measure time for filtering with parallel stream
        long startTimeParallel = System.currentTimeMillis();
        booksPublishedIn2020Parallel = books.parallelStream()
        	    .filter(book -> book.getPublished() == 2020)
        	    .collect(Collectors.toList());

        long endTimeParallel = System.currentTimeMillis();

        // Print results for parallel stream
        //System.out.println("Books published in 2020 (Parallel): " + booksPublishedIn2020Parallel);
        System.out.println("Time taken (Parallel): " + (endTimeParallel - startTimeParallel) + " ms");

        // Assess the speedup achieved with parallelism
        double speedup = (double) (endTimeSequential - startTimeSequential) / (endTimeParallel - startTimeParallel);
        System.out.println("Speedup: " + speedup);

        
        //7
         startTime = System.nanoTime();

        // Sort all the book titles in descending order using Java Streams
        List<String> sortedTitles = books.stream()
                .map(Book::getTitle)
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());

         endTime = System.nanoTime();

         timeElapsed = endTime - startTime;
        System.out.println("Time taken with Java Streams: " + TimeUnit.NANOSECONDS.toMillis(timeElapsed) + " ms");
        
        startTime = System.nanoTime();

        // Sort all the book titles in descending order using Java Streams
         sortedTitles = books.parallelStream()
                .map(Book::getTitle)
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());

         endTime = System.nanoTime();

         timeElapsed = endTime - startTime;
        //System.out.println("Sorted titles in descending order: " + sortedTitles);
        System.out.println("Time taken with Java parallel Streams: " + TimeUnit.NANOSECONDS.toMillis(timeElapsed) + " ms");


        
        //8
         startTime = System.nanoTime();
        List<String> matchingTitles = books.stream()
                .map(Book::getTitle)
                .filter(title -> title.toLowerCase().startsWith("we"))
                .collect(Collectors.toList());
        
        

         endTime = System.nanoTime();

         timeElapsed = endTime - startTime;
        System.out.println("Matching titles: " + matchingTitles);
        System.out.println("Time taken with Java Streams: " + TimeUnit.NANOSECONDS.toMillis(timeElapsed) + " ms");
        
         startTime = System.nanoTime();
         matchingTitles = books.parallelStream()
                .map(Book::getTitle)
                .filter(title -> title.toLowerCase().startsWith("we"))
                .collect(Collectors.toList());
        
        

         endTime = System.nanoTime();

         timeElapsed = endTime - startTime;
        System.out.println("Matching titles: " + matchingTitles);
        System.out.println("Time taken with Java parallel Streams: " + TimeUnit.NANOSECONDS.toMillis(timeElapsed) + " ms");
		
		
		
		

	}

}
