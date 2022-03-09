import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;


public class MapReduce {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: MapReduce <workers> <in_file> <out_file>");
            return;
        }

        ArrayList<String> document_list = new ArrayList<>();
        BufferedReader in_file = new BufferedReader (new FileReader (args[1].toString()));
        long dim_frag = Integer.parseInt(in_file.readLine());
        int nr_files = Integer.parseInt(in_file.readLine());
        long dim_file;
        int offset;
        int nr_workers = Integer.parseInt(args[0]);
        ExecutorService mapExecutor = Executors.newFixedThreadPool(nr_workers);
        ExecutorService reduceExecutor = Executors.newFixedThreadPool(nr_workers);
        ArrayList<Dictionary> dictionaryArray = new ArrayList<>();
        List<DictionaryEntry> mapResults = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger mapTaskControl = new AtomicInteger(0);
        AtomicInteger reduceTaskControl = new AtomicInteger(0);
        List<Rank> final_rankings = Collections.synchronizedList(new ArrayList<>());

        for(int i = 0; i < nr_files; ++i) {
            document_list.add(in_file.readLine());
        }
        for (String file : document_list) {
            //deschiderea fiecarui fisier pentru a crea task-urile de Map
            Dictionary dict = new Dictionary(file, new ArrayList<>(), new ArrayList<>());
            dictionaryArray.add(dict);
            File f = new File(file);
            dim_file = f.length();
            offset = 0;
            //crearea si trimiterea task-urilor de Map
            while(dim_file >= dim_frag) {
                mapTaskControl.incrementAndGet();
                mapExecutor.submit(new MapRunnable(file, offset, dim_frag, mapExecutor, f.length(), mapResults, mapTaskControl));
                offset += dim_frag;
                dim_file -= dim_frag;
            } 
            if(dim_file != 0) {
            mapTaskControl.incrementAndGet();
            mapExecutor.submit(new MapRunnable(file, offset, dim_file, mapExecutor, f.length(), mapResults, mapTaskControl));
            }
        }
        //asteptarea incheierii tuturor task-urilor de Map
        mapExecutor.awaitTermination(1000, TimeUnit.SECONDS);

        //crearea si trimiterea task-urilor de Reduce
        for (DictionaryEntry entry : mapResults) {
            for (Dictionary dict : dictionaryArray) {
                if(entry.file.equals(dict.file)) {
                    dict.frequency_v.add(entry.map);
                    dict.longest_words.add(entry.max_words);
                }
            }
        }
        for(Dictionary dict : dictionaryArray) {
            reduceTaskControl.incrementAndGet();
            reduceExecutor.submit(new ReduceRunnable(dict, reduceExecutor, reduceTaskControl, final_rankings));
        }
        //asteptarea incheierii tuturor task-urilor de Reduce
        reduceExecutor.awaitTermination(1000, TimeUnit.SECONDS);

        //sortarea si scrierea in out_file a rezultatelor
        Collections.sort(final_rankings);

        FileWriter out_file = new FileWriter(args[2]);
        PrintWriter pw = new PrintWriter(out_file);
        for(Rank rank : final_rankings) {
            String[] split_filename = rank.file.split("/");
            String actual_filename = split_filename[split_filename.length-1]; 
            pw.print(actual_filename + "," + String.format("%.2f", rank.rank) + "," + rank.max_word_length + "," + rank.nr_max_word_length + "\n");
        }
        pw.close();
    }
}

//clasa care se ocupa de rezolvarea task-urilor de Map
class MapRunnable implements Runnable {
            
    String file;
    int offset;
    long dim_frag;
    ExecutorService mapExecutor;
    long file_length;
    List<DictionaryEntry> d;
    AtomicInteger mapTaskControl;

    public ArrayList<String> max_words = new ArrayList<>();
    public HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
    int max_word_length = 0;

    public MapRunnable (String file, int offset, long dim_frag, ExecutorService mapExecutor, long file_length, List<DictionaryEntry> d, AtomicInteger mapTaskControl) {
        this.file = file;
        this.offset = offset;
        this.dim_frag = dim_frag;
        this.mapExecutor = mapExecutor;
        this.file_length = file_length;
        this.d = d;
        this.mapTaskControl = mapTaskControl;
    }

    //daca se gaseste un cuvant in secventa, updateaza vectorul de aparitii, verifica daca 
    //este de lungime maxima, daca da, updateaza lungimea maxima si il adauga in dictionar
    public void addToDictionary(String string, int word_length) {
        
        if (word_length == max_word_length) {
            max_words.add(string);
        } else if (word_length > max_word_length) {
            max_words.clear();
            max_words.add(string);
            max_word_length = word_length;
            
        }
        if(word_length > 0) {
            if(map.containsKey(word_length)) {
                map.replace(word_length, map.get(word_length) + 1);
            } else {
                map.put(word_length, 1);
            }
        }

    }
public void run(){
    try {
        String separators =  ";:/?~\\.,><`[]{}()!@#$%^&-_+'=*\"| \t\r\n";
        int valid = 1;
        int finished = 0;
        int length = 0;
        int length_word = 0;

        BufferedReader f = new BufferedReader(new FileReader(file));
        //citeste caracter cu caracter din fisierul oferit
        char[] tmp = new char[1];
        String str = new String();
        //pentru a verifica daca o secventa incepe la mijlocul unui cuvant, trebuie sa incepem citirea de la offset - 1 (exceptie offset 0),
        //iar daca acel caracter nu este separator, nu se va valida cuvantul in secventa respectiva
        if(offset != 0) {
            f.skip(offset - 1);
            f.read(tmp, 0, 1);
            if(!separators.contains(Character.toString(tmp[0]))) valid = 0;
            else valid = 1;
        }
        else {
            f.skip(offset);
        }
        while (finished == 0) {
            //daca nu am ajuns inca la finalul fragmentului cu citirea
            if(length < dim_frag - 1) {
                f.read(tmp, 0, 1);
                //daca e nu e separator, atata timp cat este valid cuvantul, se adauga caracterul in sir
                if(!separators.contains(Character.toString(tmp[0]))) {
                    if(valid == 1) {
                        str += tmp[0];     
                        length_word++;
                    }
                } 
                //daca este separator, se adauga la dictionar
                else {
                    if(str.length() != 0) {
                        addToDictionary(str, length_word);
                        str = "";
                        length_word = 0;
                    }
                    valid = 1;
                }
                length++;
            } 
            //daca depasim lungimea fragementului
            else {
                f.read(tmp, 0, 1);
                if(!separators.contains(Character.toString(tmp[0]))) {
                    if(valid == 1) {
                        str += tmp[0];  
                        length_word++;   
                    }
                    length++;
                } 
                //daca am trecut de lungimea fragmentului si dam de un separator, atunci citirea s-a incheiat
                else {
                    if(str.length() != 0) {
                        addToDictionary(str, length_word);
                        str = "";
                        length_word = 0;
                    }
                    valid = 1;
                    finished = 1;
                }
            }
            //daca dam de finalul fisierului, incheiem citirea
            if(offset + length == file_length) {
                addToDictionary(str, length_word);
                length_word = 0;
                finished = 1;
            }
        }
        //se creeaza structura care va fi trimisa la coordonator
        DictionaryEntry entry = new DictionaryEntry(file, map, max_words);
        d.add(entry);
    
        //inchidem fisierul si verificam daca putem incheia executia de Map (daca nu mai apar alte task-uri)
        f.close();
        int left = mapTaskControl.decrementAndGet();
        if(left == 0) {
            mapExecutor.shutdown();
        }
    } catch (IOException e) {
        e.printStackTrace();
    } 
}
}

    //clasa care se ocupa de rezolvarea task-urilor de Reduce
    class ReduceRunnable implements Runnable {
        Dictionary d;
        ExecutorService reduceExecutor;
        AtomicInteger reduceTaskControl;
        List<Rank> rankings;
        public ReduceRunnable (Dictionary d, ExecutorService reduceExecutor, AtomicInteger reduceTaskControl, List<Rank> rankings) {
            this.d = d;
            this.reduceExecutor = reduceExecutor;
            this.reduceTaskControl = reduceTaskControl;
            this.rankings = rankings;
        }

        public HashMap<Integer, Integer> final_frequency_v = new HashMap<Integer, Integer>();
        public ArrayList<String> final_max_words = new ArrayList<>();
        public int max_length = 0;
        public int nr_max_length = 0;
        public double rang = 0;
        public int nr_elem = 0;

        //calculul sirului lui Fibonacci
        public int fibonacci (int n) {
            int a = 0;
            int b = 1;
            int c = 0;
            for (int i = 1; i <= n; ++i) {
                c = a + b;
                a = b;
                b = c;
            }
            return c;
        }
        @Override
        public void run() {
            //inceperea etapei de combinare
            //combinarea vectorilor locali de aparitie intr-un vector final de aparitii
            for(HashMap <Integer,Integer> map : d.frequency_v) {
                if(!map.isEmpty()) {
                    for(Integer i : map.keySet()) {
                        if(final_frequency_v.containsKey(i)) {
                            final_frequency_v.replace(i, final_frequency_v.get(i) + map.get(i));
                        } else {
                            final_frequency_v.put(i, map.get(i));
                        }
                    }
                }
            }
            //combinarea listelor de cuvinte maxime intr-o lista finala de cuvinte maxime
            for(List<String> lista : d.longest_words) {
                for(String s : lista) {
                    if (s.length() == max_length) {
                        final_max_words.add(s);
                        nr_max_length++;
                    } else if (s.length() > max_length) {
                        final_max_words.clear();
                        final_max_words.add(s);
                        nr_max_length = 1;
                        max_length = s.length();
                    }
                }
            }
            //finalul etapei de combinare

            //inceputul etapei de procesare
            //pentru fiecare lungime de cuvant, se calculeaza fibonacci, si se tine cont de numarul de elemente
            for(Integer i : final_frequency_v.keySet()) {
                rang += fibonacci(i) * final_frequency_v.get(i);
                nr_elem += final_frequency_v.get(i); 
            }
            //calcularea rangului si crearea structurii care va fi trimisa catre coordonator
            rang = rang / nr_elem;
            Rank r = new Rank(d.file, rang, max_length, nr_max_length);
            rankings.add(r);
            //finalul etapei de procesare

            //verificam daca putem incheia executia de Reduce (daca nu mai apar alte task-uri)
            int left = reduceTaskControl.decrementAndGet();
            if(left == 0) {
                reduceExecutor.shutdown();
            }
        }

     }

    //structura task-urilor trimise etapei de Reduce, contine numele fisierului in care va lucra, lista de hashmap-uri care reprezinta
    //vectorii de aparitii, si o lista de liste de cuvinte care reprezinta cele mai lungi cuvinte gasite local
    class Dictionary {
        String file;
        ArrayList<HashMap<Integer, Integer>> frequency_v;
        ArrayList<List<String>> longest_words;
        public Dictionary(String file, ArrayList<HashMap<Integer, Integer>> frequency_v, ArrayList<List<String>> longest_words) {
            this.file = file;
            this.frequency_v = frequency_v;
            this.longest_words = longest_words;
        }
    }
    //structura rezultat a operatiilor de Map, contine numele fisierului in care a lucrat, hashmap-ul local care reprezinta
    //vectorul local de aparitii, si lista cu cele mai lungi cuvinte gasite local
    class DictionaryEntry {
        String file;
        HashMap<Integer, Integer> map;
        List<String> max_words;
        public DictionaryEntry(String file, HashMap<Integer, Integer> map, List<String> max_words) {
            this.file = file;
            this.map = map;
            this.max_words = max_words;
        }
    }
    
    //structura rezultat a operatiilor de Reduce, contine fisierul pe care il contorizeaza, rangul calculat, lungimea maxima a cuvantului gasita,
    //si numarul de aparitii a lungimii maxime
    class Rank implements Comparable<Rank> {
        String file;
        double rank;
        int max_word_length;
        int nr_max_word_length;
        public Rank (String file, double rank, int max_word_length, int nr_max_word_length) {
            this.file = file;
            this.rank = rank;
            this.max_word_length = max_word_length;
            this.nr_max_word_length = nr_max_word_length;
        }
        public int compareTo(Rank r1) {
            if( r1.rank >= this.rank) {
                return 1;
            } else if ( r1.rank <= this.rank) {
                return -1;
            } else {
                return 0;
            }
        }
    }
