const Mailgun = require('mailgun.js')
const FormData = require('form-data')
const dotenv = require('dotenv')
dotenv.config()

async function sendSimpleMessage(subejct, body, email, context) {
  const mailgun = new Mailgun(FormData);
  const mg = mailgun.client({
    username: "api",
    key: process.env.MAILGUN_KEY,
  });
  try {
    const data = await mg.messages.create("curbsuite.net", {
      from: "CurbSuite <postmaster@curbsuite.net>",
      to: ["Alexander James Milliken <curbsuite@gmail.com>"],
      subject: `${subejct}`,
      text: `${body}`,
    });
    const messageBody = context === 'order' ? "Thank you so much for signing up for CurbSuite's free trial! Someone will reach out to you shortly!" : 'Thank you so much for reaching out to CurbSuite we will get back to you shortly!'
    const sendToCustomer = await mg.messages.create("curbsuite.net", {
      from: "CurbSuite <postmaster@curbsuite.net>",
      to: [`Curbsuite Customer <${email}>`],
      subject: `CurbSuite Contact`,
      text: `${messageBody}`,
    });

    console.log(data); // logs response data
    console.log(sendToCustomer); // logs response data
  } catch (error) {
    console.log(error); //logs any error
  }
}

module.exports = {sendSimpleMessage}