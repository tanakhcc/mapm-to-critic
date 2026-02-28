//! This 'script' takes the MAPM data in simple format
//! (found at https://github.com/marcstober/miqra-data/blob/master/miqra-json-simple/MAM-ChamMeg.json)
//! and turns it into critic-tei-xml

mod db;

use clap::Parser;
use std::{collections::HashMap, fs::File, io::BufReader, path::PathBuf};

use dotenv::dotenv;
use std::env;

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

#[derive(Copy, Clone, Debug, Deserialize)]
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

/// Calculate the numeric value of `word`.
///
/// If characters outside the hebrew alphabet occur, they are ignored.
fn hebrew_numeral_desugar(word: &str) -> u32 {
    word.chars()
        .filter_map(|c| match c {
            'א' => Some(1_u32),
            'ב' => Some(2_u32),
            'ג' => Some(3_u32),
            'ד' => Some(4_u32),
            'ה' => Some(5_u32),
            'ו' => Some(6_u32),
            'ז' => Some(7_u32),
            'ח' => Some(8_u32),
            'ט' => Some(9_u32),
            'י' => Some(10_u32),
            'כ' => Some(20_u32),
            'ל' => Some(30_u32),
            'מ' => Some(40_u32),
            'נ' => Some(50_u32),
            'ס' => Some(60_u32),
            'ע' => Some(70_u32),
            'פ' => Some(80_u32),
            'צ' => Some(90_u32),
            'ק' => Some(100_u32),
            'ר' => Some(200_u32),
            'ש' => Some(300_u32),
            'ת' => Some(400_u32),
            'ך' => Some(500_u32),
            'ם' => Some(600_u32),
            'ן' => Some(700_u32),
            'ף' => Some(800_u32),
            'ץ' => Some(900_u32),
            _ => None,
        })
        .sum()
}

#[derive(Debug, Deserialize)]
struct SimpleMapmNumerals(Vec<MapmBookWithNumeralChaps>);

#[derive(Debug)]
struct SimpleMapm(Vec<MapmBook>);
impl TryFrom<SimpleMapmNumerals> for SimpleMapm {
    type Error = ConversionError;

    fn try_from(value: SimpleMapmNumerals) -> Result<Self, Self::Error> {
        Ok(Self(
            value
                .0
                .into_iter()
                .map(|x| x.try_into())
                .collect::<Result<Vec<_>, _>>()?,
        ))
    }
}

#[derive(Debug, Deserialize)]
struct MapmBookWithNumeralChaps {
    book_name: String,
    sub_book_name: Option<String>,
    chapters: HashMap<String, MapmChapterNumeralVerses>,
}
impl MapmBookWithNumeralChaps {
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
}
impl TryFrom<MapmBookWithNumeralChaps> for MapmBook {
    type Error = ConversionError;

    fn try_from(value: MapmBookWithNumeralChaps) -> Result<Self, Self::Error> {
        let book = value.english_name()?;
        let mut chapters = Vec::with_capacity(value.chapters.len());
        chapters.resize_with(value.chapters.len(), || None);

        for (number, content) in value.chapters {
            let position = hebrew_numeral_desugar(&number);
            match chapters.get(position as usize - 1) {
                // the chapter is after the end of the book
                None => {
                    return Err(ConversionError::ChaptersNotSuccessive(book, position));
                }
                // chapter number is in the correct range, and has not been inserted to yet
                Some(None) => {
                    let denumeralized_content: MapmChapter = content
                        .try_into()
                        .map_err(|e| ConversionError::ChapterConversion(book, position, e))?;
                    chapters[position as usize - 1] = Some(denumeralized_content);
                }
                // the chapter was already defined
                Some(Some(_existing_chapter)) => {
                    return Err(ConversionError::DuplicateChapter(book, position));
                }
            }
        }

        Ok(Self {
            book,
            chapters: chapters.into_iter().filter_map(|s| s).collect(),
        })
    }
}

#[derive(Debug)]
struct MapmBook {
    book: EnglishBook,
    chapters: Vec<MapmChapter>,
}
impl MapmBook {
    /// A list of the number of verses per chapter in this book
    fn chapter_enumeration(&self) -> Vec<u8> {
        self.chapters.iter().map(|c| c.0.len() as u8).collect()
    }

    fn to_streamed_book(self) -> Result<Book, ConversionError> {
        let mut total_versified_content = Vec::with_capacity(self.chapters.len());

        let mut current_chapter = 0;
        for chapter in self.chapters.into_iter() {
            current_chapter += 1;
            let mut current_verse = 0;
            let mut chapter_content = Vec::new();
            for verse in chapter.0.into_iter() {
                current_verse += 1;
                chapter_content.push(critic_format::streamed::Block::Anchor(
                    critic_format::normalized::Anchor {
                        anchor_type: "Masoretic".to_string(),
                        anchor_id: format!(
                            "A_V_MT_{}-{current_chapter}-{current_verse}",
                            self.book.english_name()
                        ),
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
            name: Some(self.book),
            chapters: total_versified_content,
        })
    }
}

/// Remove all <span> ... </span> elements in input
fn span_clean(input: String) -> String {
    let re = regex::Regex::new("<span.*>.*</span>").expect("Static Regex");
    let res = re.replace_all(&input, "");
    if let std::borrow::Cow::Owned(x) = res {
        x
    } else {
        input
    }
}

#[derive(Default, Debug, Deserialize)]
struct MapmChapter(Vec<String>);

#[derive(Default, Debug, Deserialize)]
struct MapmChapterNumeralVerses(HashMap<String, String>);
impl TryFrom<MapmChapterNumeralVerses> for MapmChapter {
    type Error = ChapterConversionError;

    fn try_from(value: MapmChapterNumeralVerses) -> Result<Self, Self::Error> {
        let mut res = Vec::with_capacity(value.0.len());
        res.resize_with(value.0.len(), || None);

        for (hebrew_numeral, content) in value.0 {
            let position = hebrew_numeral_desugar(&hebrew_numeral);
            match res.get(position as usize - 1) {
                // Verse with this number does not exist
                None => {
                    return Err(ChapterConversionError::LinesNotSuccessive(position));
                }
                Some(None) => {
                    res[position as usize - 1] = Some(span_clean(content));
                }
                Some(Some(_content)) => {
                    return Err(ChapterConversionError::DuplicateLine(position));
                }
            }
        }
        Ok(Self(res.into_iter().filter_map(|s| s).collect()))
    }
}

#[derive(Debug)]
enum ChapterConversionError {
    LinesNotSuccessive(u32),
    DuplicateLine(u32),
}
impl core::fmt::Display for ChapterConversionError {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            ChapterConversionError::LinesNotSuccessive(line) => {
                write!(
                    f,
                    "The line {line} exists, but there are less lines in total in this chapter."
                )
            }
            ChapterConversionError::DuplicateLine(line) => {
                write!(f, "The chapter contains {line} twice.")
            }
        }
    }
}
impl core::error::Error for ChapterConversionError {}

#[derive(Debug)]
enum ConversionError {
    BookTitleUnknown(String),
    ChaptersNotSuccessive(EnglishBook, u32),
    DuplicateChapter(EnglishBook, u32),
    ChapterConversion(EnglishBook, u32, ChapterConversionError),
}
impl core::fmt::Display for ConversionError {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            ConversionError::BookTitleUnknown(x) => {
                write!(f, "The book title {x} is not known.")
            }
            ConversionError::ChaptersNotSuccessive(book, chapter) => {
                write!(
                    f,
                    "The chapter {chapter} exists in {book}, but there are less chapters in total."
                )
            }
            ConversionError::DuplicateChapter(book, chapter) => {
                write!(f, "Book {book} contains {chapter} twice.")
            }
            ConversionError::ChapterConversion(book, chapter, inner) => {
                write!(f, "Failed to convert chapter {chapter} in {book}: {inner}")
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
    name: Option<EnglishBook>,
    chapters: Vec<(u8, Vec<critic_format::streamed::Block>)>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn core::error::Error>> {
    dotenv().ok();
    let mut connection_string = None::<String>;
    for (key, value) in env::vars() {
        if key == "DATABASE_URL" {
            connection_string = Some(value);
            break;
        }
    }
    if connection_string.is_none() {
        panic!("I need a connection_string to the DB in the .env file like for use with sqlx.");
    }
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
                let mapm_content: SimpleMapmNumerals = match serde_json::from_reader(buf_reader) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("Error trying to parse {p:?}: {e:?}");
                        return Err(e)?;
                    }
                };
                let mapm_content: SimpleMapm = mapm_content.try_into()?;
                for book in mapm_content.0.into_iter() {
                    let book_name = book.book;
                    let streamed_book = book.to_streamed_book()?;
                    corpus.books[book_name as usize - 1] = (0, streamed_book);
                }
            }
            Err(e) => {
                eprintln!("Error while reading file.");
                return Err(e)?;
            }
        }
    }

    let pool =
        sqlx::Pool::connect(&connection_string.expect("Should have connection string now")).await?;
    db::insert(&pool, corpus).await?;
    Ok(())
}
