const Mailgun = require('mailgun.js')
const FormData = require('form-data')
const dotenv = require('dotenv')
dotenv.config()

async function sendSimpleMessage(subejct, body) {
  const mailgun = new Mailgun(FormData);
  const mg = mailgun.client({
    username: "api",
    key: process.env.MAILGUN_KEY,
  });
  try {
    const data = await mg.messages.create("curbsuite.net", {
      from: "Mailgun Sandbox <postmaster@curbsuite.net>",
      to: ["Alexander James Milliken <curbsuite@gmail.com>"],
      subject: `${subejct}`,
      text: `${body}`,
    });

    console.log(data); // logs response data
  } catch (error) {
    console.log(error); //logs any error
  }
}

module.exports = {sendSimpleMessage}