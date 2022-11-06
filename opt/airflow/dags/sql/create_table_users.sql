CREATE TABLE IF NOT EXISTS staging.users_file (
	uri text ,
	name text ,
	image_url text ,
	followers_count int4 ,
	following_count int4 ,
	public_playlist text,
	total_public_playlist_count int4,
	has_spotify_name bool,
	has_spotify_image bool,
	color int4,
	user_created_show text
);