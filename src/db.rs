//! Insertions into the DB from fully parsed data.

use itertools::Itertools;
use sqlx::{Pool, Postgres};

use crate::{Book, Corpus};

/// Contains content as XML String and an iterator of verse-name - verse id
struct Chunk<I>
where
    I: IntoIterator<Item = (String, i64)>,
{
    content: String,
    verses: I,
}

async fn insert_chunk<I>(
    pool: &Pool<Postgres>,
    chunk: Chunk<I>,
    versification_scheme_id: i64,
    language_id: i64,
) -> Result<(), sqlx::Error>
where
    I: IntoIterator<Item = (String, i64)>,
{
    // insert the verses into the verse map
    let new_verse_ids = insert_verses(pool, chunk.verses, versification_scheme_id).await?;
    if new_verse_ids.is_empty() {
        return Ok(());
    }
    // insert the content into the base_corpus table
    sqlx::query!(
        "INSERT INTO base_corpus (language, versification_scheme, content, verse_start, verse_end) VALUES ($1, $2, $3, $4, $5);",
        language_id,
        versification_scheme_id,
        chunk.content,
        new_verse_ids.iter().min(),
        new_verse_ids.iter().max(),)
        .execute(pool)
        .await
        .map(|_| ())
}

/// the input verses should be the verse numbers in the MT scheme
async fn insert_verses(
    pool: &Pool<Postgres>,
    verses: impl IntoIterator<Item = (String, i64)>,
    versification_scheme_id: i64,
) -> Result<Vec<i64>, sqlx::Error> {
    let mut res = Vec::new();
    for verse in verses {
        let new_id = sqlx::query!("INSERT INTO verse DEFAULT VALUES RETURNING id;")
            .fetch_one(pool)
            .await?
            .id;
        sqlx::query!(
            "INSERT INTO verse_map (verse_id, versification_scheme, verse_nr, verse_monotone_id) VALUES ($1, $2, $3, $4);",
            new_id,
            versification_scheme_id,
            verse.0,
            verse.1 as i64,
        )
        .execute(pool)
        .await?;
        res.push(new_id);
    }
    Ok(res)
}

/// Given a chapter with `number_of_verses` verses, chunk it into this length of chunk
fn divide_into_good_chunks(number_of_verses: usize) -> usize {
    if number_of_verses % 11 >= 3 || number_of_verses % 11 == 0 {
        11
    } else if number_of_verses % 10 >= 3 || number_of_verses % 10 == 0 {
        10
    } else {
        9
    }
}

async fn insert_chapter(
    pool: &Pool<Postgres>,
    starting_verse_id: i64,
    chapter: Vec<critic_format::streamed::Block>,
    versification_scheme_id: i64,
    language_id: i64,
) -> Result<(), sqlx::Error> {
    println!("Now inserting new chapter: {:?}", chapter[0]);
    // calculate the right splitting behaviour
    let chunk_size = divide_into_good_chunks(
        chapter
            .iter()
            .filter(|b| b.block_type() == critic_format::streamed::BlockType::Anchor)
            .count(),
    );
    let mut current_verse = starting_verse_id;

    // the verse markup is always one block long, the content one further block
    for chunk in &chapter.into_iter().chunks(2 * chunk_size) {
        let mut verses = Vec::with_capacity(chunk_size);
        let content = critic_format::page_to_xml(
            // this is inside a map for the side effect to avoid cloning the blocks
            chunk.map(|block| {
                if let critic_format::streamed::Block::Anchor(ref a) = block {
                    verses.push((a.anchor_id.clone(), current_verse));
                    current_verse += 1;
                }
                block
            }),
            format!("MAPM from verse {starting_verse_id}"),
        )
        .expect("Known static structure of Blocks");

        let final_chunk = Chunk { content, verses };
        insert_chunk(pool, final_chunk, versification_scheme_id, language_id).await?;
    }
    Ok(())
}

async fn insert_book(
    pool: &Pool<Postgres>,
    starting_verse_id: u32,
    book: Book,
    versification_scheme_id: i64,
    language_id: i64,
) -> Result<(), sqlx::Error> {
    println!("Now inserting book {:?}", book.name);
    let mut current_verse = starting_verse_id;
    for chapter in book.chapters {
        insert_chapter(
            pool,
            current_verse.into(),
            chapter.1,
            versification_scheme_id,
            language_id,
        )
        .await?;
        current_verse += u32::from(chapter.0);
    }
    Ok(())
}

async fn insert_versification_scheme(pool: &Pool<Postgres>) -> Result<i64, sqlx::Error> {
    Ok(sqlx::query!(
        r#"WITH e AS (INSERT INTO versification_scheme (full_name, shorthand) VALUES ('Masoretic', 'MT') ON CONFLICT DO NOTHING RETURNING id)
        SELECT id as "id!" FROM e UNION SELECT id FROM versification_scheme WHERE full_name = 'Masoretic' and shorthand = 'MT';"#
    )
    .fetch_one(pool)
    .await?.id)
}

async fn insert_language(pool: &Pool<Postgres>) -> Result<i64, sqlx::Error> {
    Ok(sqlx::query!(
        r#"WITH e AS (INSERT INTO language (name, equality_alphabet) VALUES ('hbo', 'אבגדהוזחטיךכלםמןנסעףפץצקרשת') ON CONFLICT DO NOTHING RETURNING id)
        SELECT id as "id!" FROM e UNION SELECT id FROM language WHERE name = 'hbo';"#
    )
    .fetch_one(pool)
    .await?.id)
}

/// Insert the complete corpus into the db
pub async fn insert(pool: &Pool<Postgres>, corpus: Corpus) -> Result<(), sqlx::Error> {
    // insert the versification scheme
    let versification_scheme_id = insert_versification_scheme(pool).await?;
    // insert the language
    let language_id = insert_language(pool).await?;

    for book in corpus.books {
        insert_book(pool, book.0, book.1, versification_scheme_id, language_id).await?;
    }
    Ok(())
}
