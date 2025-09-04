import pluginFilters from "./src/_config/filters.js";
import { eleventyImageTransformPlugin } from "@11ty/eleventy-img";
import { EleventyI18nPlugin } from "@11ty/eleventy";
import pluginIcons from "eleventy-plugin-icons";
import fs from "fs";
import path from "path";
import zlib from "zlib";
import pluginRss from "@11ty/eleventy-plugin-rss";
import lucideIcons from "@grimlink/eleventy-plugin-lucide-icons";
import EleventyPluginOgImage from "eleventy-plugin-og-image";

export default async function (eleventyConfig) {
  const outputDir = "_site";

  eleventyConfig.addPassthroughCopy({
    "./src/assets/favicon": "assets/favicon",
  });
  eleventyConfig.addPassthroughCopy({ "./src/assets/img": "assets/img" });
  eleventyConfig.addPassthroughCopy({ "./src/assets/js": "assets/js" });
  eleventyConfig.addPassthroughCopy({ "./src/assets/css": "assets/css" });
  eleventyConfig.addPassthroughCopy({ "./src/data/": "data" });
  eleventyConfig.addPassthroughCopy({ "./src/metadata/": "metadata" });
  eleventyConfig.addPassthroughCopy("robots.txt");

  // Filters
  eleventyConfig.addPlugin(pluginRss);
  eleventyConfig.addPlugin(pluginFilters);

  eleventyConfig.addPlugin(EleventyI18nPlugin, {
    defaultLanguage: "nl",
  });

  eleventyConfig.addPlugin(lucideIcons, {
    class: "icon",
    width: 16,
    height: 16,
  });

  eleventyConfig.addPlugin(EleventyPluginOgImage, {
    satoriOptions: {
      fonts: [
        {
          name: "Inter",
          data: fs.readFileSync(
            "../web/src/assets/fonts/arial-rounded-bold.woff",
          ),
          weight: 700,
          style: "normal",
        },
      ],
    },
  });

  // eleventyConfig.on("eleventy.after", brotli_compress_text_files)

  // PAGEFIND
  eleventyConfig.on("eleventy.after", async function ({ dir }) {
    const inputPath = dir.output;
    const outputPath = path.join(dir.output, "pagefind");

    console.log("Creating Pagefind index of %s", inputPath);

    const pagefind = await import("pagefind");
    const { index } = await pagefind.createIndex();
    const { page_count } = await index.addDirectory({ path: inputPath });
    await index.writeFiles({ outputPath });

    console.log(
      "Created Pagefind index of %i pages in %s",
      page_count,
      outputPath,
    );
  });

  // Collections.
  eleventyConfig.addCollection("localizedMeetings", function (collectionApi) {
    const localized = [];
    const langs = ["nl"]; //,"fr"];
    const meetings = collectionApi.items[0].data.meetings;
    for (const meeting of meetings.meetings) {
      if (meeting.type === "plenary") {
        for (const lang of langs) {
          localized.push({
            ...meeting,
            lang,
            permalink:
              `/${lang}/meetings/${meeting.session_id}/${meeting.meeting_id}/`,
          });
        }
      }
    }
    return localized;
  });

  eleventyConfig.addCollection(
    "localizedCommissions",
    function (collectionApi) {
      const localized = [];
      const langs = ["nl"]; //,"fr"];
      const meetings = collectionApi.items[0].data.meetings;
      for (const commission of meetings.meetings) {
        if (commission.type === "commission") {
          for (const lang of langs) {
            localized.push({
              ...commission,
              lang,
              permalink:
                `/${lang}/commissions/${commission.session_id}/${commission.meeting_id}/`,
            });
          }
        }
      }
      return localized;
    },
  );

  eleventyConfig.addCollection("localizedMembers", function (collectionApi) {
    const localized = [];
    const langs = ["nl"]; //, "fr", "de"];
    const members = collectionApi.items[0].data.members;
    for (const member of members.members) {
      for (const lang of langs) {
        localized.push({
          ...member,
          lang,
          permalink: `/${lang}/members/${string_to_slug(member.first_name)}-${
            string_to_slug(member.last_name)
          }/`,
          title:
            `${member.first_name.toLowerCase()} ${member.last_name.toLowerCase()}`,
        });
      }
    }
    return localized;
  });

  eleventyConfig.addCollection("localizedParties", function (collectionApi) {
    const localized = [];
    const langs = ["nl"]; //, "fr", "de"];
    const parties = collectionApi.items[0].data.parties.parties;
    const partyKeys = Object.keys(parties);
    for (const partyKey of partyKeys) {
      for (const lang of langs) {
        localized.push({
          ...parties[partyKey],
          lang,
          permalink: `/${lang}/parties`,
        });
      }
    }
    return localized;
  });

  eleventyConfig.addCollection("localizedQuestions", function (collectionApi) {
    const localized = [];
    const langs = ["nl"]; //, "fr", "de"];
    const questions = collectionApi.items[0].data.questions;
    for (const question of questions.questions) {
      if (question.type === "plenary") {
        for (const lang of langs) {
          localized.push({
            ...question,
            lang,
            permalink:
              `/${lang}/questions/${question.session_id}/${question.meeting_id}/${question.question_id}/`,
          });
        }
      }
    }
    return localized;
  });

  eleventyConfig.addCollection("localizedDossiers", function (collectionApi) {
    const localized = [];
    const langs = ["nl"]; //, "fr", "de"];
    const dossiers = collectionApi.items[0].data.dossiers;
    for (const dossier of dossiers.dossiers) {
      for (const lang of langs) {
        localized.push({
          ...dossier,
          lang,
          permalink:
            `/${lang}/sessions/${dossier.session_id}/dossiers/${dossier.dossier_id}/`,
        });
      }
    }
    return localized;
  });

  eleventyConfig.addCollection(
    "localizedPlenaryVotes",
    function (collectionApi) {
      //console.log(collectionApi.items[0].data.meetings);
      const localized = [];
      const langs = ["nl"]; //, "fr", "de"];
      const meetings = collectionApi.items[0].data.meetings;
      for (const meeting of meetings.meetings) {
        if (meeting.type === "plenary") {
          for (const lang of langs) {
            for (const vote of meeting.allVotes) {
              localized.push({
                ...vote,
                lang,
                permalink:
                  `/${lang}/sessions/${meeting.session_id}/meetings/plenary/${meeting.meeting_id}/votes/${vote.vote_id}/`,
              });
            }
          }
        }
      }
      return localized;
    },
  );

  //   eleventyConfig.addCollection("localizedTopics", function(collectionApi) {
  //     const localized = [];
  //     const langs = ["nl", "fr"]; // , "fr", "de"];
  //     const topicsData = collectionApi.items[0].data.topics;

  //     for (const mainTopicKey of Object.keys(topicsData)) {
  //         const mainTopicData = topicsData[mainTopicKey];

  //         for (const lang of langs) {
  //             // Add the main topic itself
  //             localized.push({
  //                 type: "main",
  //                 lang,
  //                 name: mainTopicKey,
  //                 label: mainTopicData[lang] || mainTopicKey,  // human-readable label
  //                 icon: mainTopicData.icon || "",
  //                 parent: null,
  //             });

  //             // Add subtopics if any
  //             if (mainTopicData.subtopics) {
  //                 for (const subtopicName of Object.keys(mainTopicData.subtopics)) {
  //                     localized.push({
  //                         type: "subtopic",
  //                         lang,
  //                         name: subtopicName,
  //                         label: subtopicName, // no translation yet, using subtopic name directly
  //                         icon: mainTopicData.icon || "", // inherit icon if you want
  //                         parent: mainTopicKey, // parent topic
  //                     });
  //                 }
  //             }
  //         }
  //     }

  //     return localized;
  // });

  eleventyConfig.addCollection("localizedTopics", function (collectionApi) {
    const localized = [];
    const langs = ["nl"]; // later also "fr", "de"
    const topicsData = collectionApi.items[0].data.topics;

    for (const mainTopicKey of Object.keys(topicsData)) {
      const mainTopicData = topicsData[mainTopicKey];

      for (const lang of langs) {
        // Main topic
        localized.push({
          type: "main",
          lang,
          name: mainTopicKey,
          label: mainTopicData[lang] || mainTopicKey,
          icon: mainTopicData.icon || "",
          parent: null,
          keywords: mainTopicData.keywords || [],
        });

        // Subtopics
        if (mainTopicData.subtopics) {
          for (const subtopicKey of Object.keys(mainTopicData.subtopics)) {
            localized.push({
              type: "subtopic",
              lang,
              name: subtopicKey,
              label: subtopicKey, // could later add translation
              icon: mainTopicData.icon || "",
              parent: mainTopicKey,
              keywords: mainTopicData.subtopics[subtopicKey] || [],
            });
          }
        }
      }
    }

    return localized;
  });

  // Plugins
  eleventyConfig.addPlugin(eleventyImageTransformPlugin);
  eleventyConfig.addPlugin(pluginIcons, {
    mode: "inline",
    sources: [{ name: "lucide", path: "node_modules/lucide-static/icons" }],
    icon: {
      shortcode: "icon",
      delimiter: ":",
      transform: async (content) => content,
      class: (name, source) => `icon icon-${name}`,
      id: (name, source) => `icon-${name}`,

      attributes: {
        width: "16",
        height: "16",
      },

      attributesBySource: {},
      overwriteExistingAttributes: true,
      errorNotFound: true,
    },
    sprite: {
      shortcode: "spriteSheet",
      attributes: {
        class: "sprite-sheet",
        "aria-hidden": "true",
        xmlns: "http://www.w3.org/2000/svg",
      },
      extraIcons: {
        all: false,
        sources: [],
        icons: [],
      },
      writeFile: false,
    },
  });

  eleventyConfig.addWatchTarget("src/_includes");
  eleventyConfig.addWatchTarget("src/content");
  eleventyConfig.addWatchTarget("./src/assets");
  //
  // eleventyConfig.addTransform("htmlmin", (content, outputPath) => {
  //   if (outputPath.endsWith(".html")) {
  //     return htmlmin.minify(content, {
  //       collapseWhitespace: true,
  //       removeComments: true,
  //       useShortDoctype: true,
  //     });
  //   }
  //
  //   return content;
  // });

  eleventyConfig.setServerOptions({
    liveReload: true,
    output: outputDir,
  });
}

function brotli_compress_text_files(_) {
  const outputDir = "_site";
  const textFileEndings = ["html", "css", "svg", "js", "json", "xml"];
  const files = fs.readdirSync(outputDir, { recursive: true });
  const textFiles = files.filter((f) =>
    textFileEndings.some((ending) => f.endsWith(ending))
  );
  textFiles.forEach((f) => write_brotli_compressed_file(f));
}

function write_brotli_compressed_file(uncompressedFilePath) {
  const outputDir = "_site";
  const outputPath = (outputDir.endsWith("/") ? outputDir : outputDir + "/") +
    uncompressedFilePath;
  const uncompressed = fs.readFileSync(outputPath);
  const compressed = zlib.brotliCompressSync(uncompressed, {
    [zlib.constants.BROTLI_PARAM_MODE]: zlib.constants.BROTLI_MODE_TEXT,
    [zlib.constants.BROTLI_PARAM_QUALITY]: zlib.constants.BROTLI_MAX_QUALITY,
  });
  fs.writeFileSync(outputPath + ".br", compressed);
}

function string_to_slug(str) {
  str = str.replace(/^\s+|\s+$/g, ""); // trim
  str = str.toLowerCase();

  // remove accents, swap ñ for n, etc
  var from = "àáäâèéëêìíïîòóöôùúüûñç·/_,:;";
  var to = "aaaaeeeeiiiioooouuuunc------";
  for (var i = 0, l = from.length; i < l; i++) {
    str = str.replace(new RegExp(from.charAt(i), "g"), to.charAt(i));
  }

  str = str
    .replace(/[^a-z0-9 -]/g, "") // remove invalid chars
    .replace(/\s+/g, "-") // collapse whitespace and replace by -
    .replace(/-+/g, "-"); // collapse dashes

  return str;
}

export const config = {
  dir: {
    input: "src/content",
    includes: "../_includes",
    data: "../_data",
    output: "_site",
  },
};
