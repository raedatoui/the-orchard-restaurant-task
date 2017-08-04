var gulp          = require('gulp');
var browserSync   = require('browser-sync').create();
var sass          = require('gulp-sass');
var autoprefixer  = require('gulp-autoprefixer');
var extend        = require('extend');
var parseArgs     = require('minimist');
var runSequence   = require('run-sequence');
var gulpif        = require('gulp-if');
var uglify        = require('gulp-uglify');

var config = extend({env: process.env.NODE_ENV}, parseArgs(process.argv.slice(2)));

gulp.task('serve', ['sass'], function() {
	browserSync.init({
		proxy: "http://localhost:5000"
	});

	gulp.watch("./src/sass/**/*.scss", ['sass']);
	gulp.watch("../templates/*.html").on('change', browserSync.reload);
});

gulp.task('sass', function() {
	return gulp.src("./src/sass/app.scss")
		.pipe(sass({outputStyle: (config.env === 'production') ? 'compressed' : ''}))
		.pipe(autoprefixer({
			browsers: ['last 2 versions'],
			cascade: false
		}))
		.pipe(gulp.dest("./css"))
		.pipe(gulpif(config.env !== 'production', browserSync.stream()))
});

gulp.task('set-development', function() {
	return process.env.NODE_ENV = config.env = 'development';
});

gulp.task('set-production', function() {
	return process.env.NODE_ENV = config.env = 'production';
});

gulp.task('build', ['sass']);

gulp.task('dev', ['set-development'], function() {
	return runSequence(
		'build'
	);
});

gulp.task('prod', ['set-production'], function() {
	return runSequence(
		'build'
	);
});

gulp.task('default', ['serve']);