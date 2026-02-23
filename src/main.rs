//! This 'script' takes the MAPM data in simple format
//! (found at https://github.com/marcstober/miqra-data/blob/master/miqra-json-simple/MAM-ChamMeg.json)
//! and turns it into critic-tei-xml

mod db;

use clap::Parser;
use std::{collections::HashMap, fs::File, io::BufReader, path::PathBuf};

use serde::Deserialize;

/// Takes the MAPM input files found in `input_directory` and transforms them into critic-tei-xml
/// data in the `output_directory`.
#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// Directory to read inputs from
    #[arg(short, long)]
    input_directory: PathBuf,
    /// Directory to write output to
    #[arg(short, long, default_value = ".")]
    output_directory: Option<PathBuf>,
}

enum EnglishBook {
    Genesis = 1,
    Exodus = 2,
    Leviticus = 3,
    Numbers = 4,
    Deuteronomy = 5,
    Joshua = 6,
    Judges = 7,
    ISamuel = 8,
    IISamuel = 9,
    IKings = 10,
    IIKings = 11,
    Isaiah = 12,
    Jeremiah = 13,
    Ezekiel = 14,
    Hosea = 15,
    Joel = 16,
    Amos = 17,
    Obadjah = 18,
    Jonah = 19,
    Micah = 20,
    Nahum = 21,
    Habakuk = 22,
    Zephaniah = 23,
    Haggai = 24,
    Zechariah = 25,
    Malachi = 26,
    Psalms = 27,
    Job = 28,
    Proverbs = 29,
    Ruth = 30,
    Song = 31,
    Kohelet = 32,
    Lamentations = 33,
    Esther = 34,
    Daniel = 35,
    Ezra = 36,
    Nehemiah = 37,
    IChronicles = 38,
    IIChronicles = 39,
}
impl core::fmt::Display for EnglishBook {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(f, "{}", self.english_name())
    }
}

impl EnglishBook {
    fn english_name(&self) -> &'static str {
        match self {
            Self::Psalms => "Psalms",
            Self::Proverbs => "Proverbs",
            Self::Job => "Job",
            Self::Genesis => "Genesis",
            Self::Exodus => "Exodus",
            Self::Leviticus => "Leviticus",
            Self::Numbers => "Numbers",
            Self::Deuteronomy => "Deuteronomy",
            Self::Song => "Song of Songs",
            Self::Ruth => "Ruth",
            Self::Lamentations => "Lamentations",
            Self::Kohelet => "Kohelet",
            Self::Esther => "Esther",
            Self::Isaiah => "Isaiah",
            Self::Jeremiah => "Jeremiah",
            Self::Ezekiel => "Ezekiel",
            Self::Hosea => "Hosea",
            Self::Joel => "Joel",
            Self::Amos => "Amos",
            Self::Obadjah => "Obadjah",
            Self::Jonah => "Jonah",
            Self::Micah => "Micah",
            Self::Nahum => "Nahum",
            Self::Habakuk => "Habakuk",
            Self::Zephaniah => "Zephaniah",
            Self::Haggai => "Haggai",
            Self::Zechariah => "Zechariah",
            Self::Malachi => "Malachi",
            Self::Daniel => "Daniel",
            Self::Ezra => "Ezra",
            Self::Nehemiah => "Nehemiah",
            Self::Joshua => "Joshua",
            Self::Judges => "Judges",
            Self::IChronicles => "IChronicles",
            Self::IIChronicles => "IIChronicles",
            Self::ISamuel => "ISamuel",
            Self::IISamuel => "IISamuel",
            Self::IKings => "IKings",
            Self::IIKings => "IIKings",
        }
    }

    /// convert mapm book names to english names
    fn book_names_to_english(name: &str, subname: Option<&str>) -> Option<Self> {
        match name {
            "ספר תהלים" => Some(EnglishBook::Psalms),
            "ספר משלי" => Some(EnglishBook::Proverbs),
            "ספר איוב" => Some(EnglishBook::Job),
            "ספר בראשית" => Some(EnglishBook::Genesis),
            "ספר שמות" => Some(EnglishBook::Exodus),
            "ספר ויקרא" => Some(EnglishBook::Leviticus),
            "ספר במדבר" => Some(EnglishBook::Numbers),
            "ספר דברים" => Some(EnglishBook::Deuteronomy),
            "מגילת שיר השירים" => Some(EnglishBook::Song),
            "מגילת רות" => Some(EnglishBook::Ruth),
            "מגילת איכה" => Some(EnglishBook::Lamentations),
            "מגילת קהלת" => Some(EnglishBook::Kohelet),
            "מגילת אסתר" => Some(EnglishBook::Esther),
            "ספר ישעיהו" => Some(EnglishBook::Isaiah),
            "ספר ירמיהו" => Some(EnglishBook::Jeremiah),
            "ספר יחזקאל" => Some(EnglishBook::Ezekiel),
            "ספר תרי עשר" => match subname {
                Some("הושע") => Some(EnglishBook::Hosea),
                Some("יואל") => Some(EnglishBook::Joel),
                Some("עמוס") => Some(EnglishBook::Amos),
                Some("עבדיה") => Some(EnglishBook::Obadjah),
                Some("יונה") => Some(EnglishBook::Jonah),
                Some("מיכה") => Some(EnglishBook::Micah),
                Some("נחום") => Some(EnglishBook::Nahum),
                Some("חבקוק") => Some(EnglishBook::Habakuk),
                Some("צפניה") => Some(EnglishBook::Zephaniah),
                Some("חגי") => Some(EnglishBook::Haggai),
                Some("זכריה") => Some(EnglishBook::Zechariah),
                Some("מלאכי") => Some(EnglishBook::Malachi),
                _ => None,
            },
            "ספר דניאל" => Some(EnglishBook::Daniel),
            "ספר עזרא" => match subname {
                Some("עזרא") => Some(EnglishBook::Ezra),
                Some("נחמיה") => Some(EnglishBook::Nehemiah),
                _ => None,
            },
            "ספר יהושע" => Some(EnglishBook::Joshua),
            "ספר שופטים" => Some(EnglishBook::Judges),
            "ספר דברי הימים" => match subname {
                Some("דה\"א") => Some(EnglishBook::IChronicles),
                Some("דה\"ב") => Some(EnglishBook::IIChronicles),
                _ => None,
            },
            "ספר שמואל" => match subname {
                Some("שמ\"א") => Some(EnglishBook::ISamuel),
                Some("שמ\"ב") => Some(EnglishBook::IISamuel),
                _ => None,
            },
            "ספר מלכים" => match subname {
                Some("מל\"א") => Some(EnglishBook::IKings),
                Some("מל\"ב") => Some(EnglishBook::IIKings),
                _ => None,
            },
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct SimpleMapm(Vec<MapmBook>);

#[derive(Debug, Deserialize)]
struct MapmBook {
    book_name: String,
    sub_book_name: Option<String>,
    chapters: HashMap<String, MapmChapter>,
}
impl MapmBook {
    fn english_name(&self) -> Result<EnglishBook, ConversionError> {
        EnglishBook::book_names_to_english(&self.book_name, self.sub_book_name.as_deref()).ok_or(
            ConversionError::BookTitleUnknown(if let Some(sub) = &self.sub_book_name {
                format!("{}:{}", self.book_name, sub)
            } else {
                self.book_name.clone()
            }),
        )
    }

    fn english_title(&self) -> Result<&'static str, ConversionError> {
        self.english_name().map(|book| book.english_name())
    }

    /// A list of the number of verses per chapter in this book
    fn chapter_enumeration(&self) -> Vec<u8> {
        self.chapters.iter().map(|c| c.1.0.len() as u8).collect()
    }

    fn to_streamed_book(self) -> Result<Book, ConversionError> {
        let bookname = self.english_title()?;

        let mut total_versified_content = Vec::with_capacity(self.chapters.len());

        let mut current_chapter = 0;
        for (_name, chapter) in self.chapters.into_iter() {
            current_chapter += 1;
            let mut current_verse = 0;
            let mut chapter_content = Vec::new();
            for (_name, verse) in chapter.0.into_iter() {
                current_verse += 1;
                chapter_content.push(critic_format::streamed::Block::Anchor(
                    critic_format::normalized::Anchor {
                        anchor_type: "Masoretic".to_string(),
                        anchor_id: format!("A_V_MT_{bookname}-{current_chapter}-{current_verse}"),
                    },
                ));
                chapter_content.push(critic_format::streamed::Block::Text(
                    critic_format::streamed::Paragraph {
                        lang: "hbo".to_string(),
                        content: verse,
                    },
                ));
            }
            total_versified_content.push((current_verse, chapter_content));
        }
        Ok(Book {
            chapters: total_versified_content,
        })
    }
}

#[derive(Debug, Deserialize)]
struct MapmChapter(HashMap<String, String>);

#[derive(Debug)]
enum ConversionError {
    BookTitleUnknown(String),
}
impl core::fmt::Display for ConversionError {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            ConversionError::BookTitleUnknown(x) => {
                write!(f, "The book title {x} is not known.")
            }
        }
    }
}
impl core::error::Error for ConversionError {}

#[derive(Debug, Deserialize, Default)]
struct Corpus {
    books: Vec<(u32, Book)>,
}
#[derive(Debug, Deserialize, Default)]
struct Book {
    chapters: Vec<(u8, Vec<critic_format::streamed::Block>)>,
}

fn main() -> Result<(), Box<dyn core::error::Error>> {
    let args = Args::parse();

    // read the input data file by file
    let paths = std::fs::read_dir(args.input_directory)?;
    let mut corpus = Corpus {
        books: (0..39).map(|_| (0, Book::default())).collect(),
    };

    for path in paths {
        match path {
            Ok(p) => {
                let file = File::open(p.path())?;
                let buf_reader = BufReader::new(file);
                let mapm_content: SimpleMapm = match serde_json::from_reader(buf_reader) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("Error trying to parse {p:?}: {e:?}");
                        return Err(Box::new(e));
                    }
                };
                for book in mapm_content.0.into_iter() {
                    let book_name = book.english_name()?;
                    let streamed_book = book.to_streamed_book()?;
                    corpus.books[book_name as usize - 1] = (0, streamed_book);
                }
            }
            Err(e) => {
                eprintln!("Error while reading file.");
                return Err(Box::new(e));
            }
        }
    }

    // TODO: actually try the insert and debug any remaining problems
    db::insert(corpus);
    Ok(())
}
